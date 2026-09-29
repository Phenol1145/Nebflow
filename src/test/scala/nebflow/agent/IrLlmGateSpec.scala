package nebflow.agent

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.syntax.*
import io.circe.JsonObject
import nebflow.core.hooks.*
import nebflow.core.tools.ToolContext
import nebflow.ir.{IrExec, IrRouteLeg, IrRoutePort}
import nebflow.shared.{ToolCall, ToolExecResult}
import nebflow.actor.ActorSystem
import munit.FunSuite

import java.nio.file.Files as JFiles
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/**
 * P1-3 改道闸（executeTool 第三前置改道）的引擎面用例：开关双态、三态映射、
 * AskFallback 单次执行、畸形/kimi 优先序、route miss、模型面名保持。
 *
 * 直钉手法 = HookExecutionIntegrationSpec 先例：CoreProbe extends AgentCore 暴露
 * protected executeTool；SharedResources 经 SpecResources 构造后 `.copy(irRoute = …)`
 * 注入 record-stub（off 腿不注入——缺省 None 即 off）。执行计数 = counting
 * HookEngine 的 beforeTool 触发数（executeToolInner 每次真实执行恰一发）。
 *
 * 双闸消解的结构面在此钉住：闸只在 executeTool 内部生效 ⇒ 旧 permissionDecision
 * 恒先于闸（三调用点全在旧面放行之后）；IR 面三态里 Ask 不发卡（回退直呼）、
 * Blocked 零执行 ⇒ 同一调用最多被问一次。
 */
class IrLlmGateSpec extends FunSuite:

  // AgentCore.executeTool is protected; expose via minimal stub (HookExecutionIntegrationSpec pattern)
  private object CoreProbe extends AgentCore:
    def exec(call: ToolCall, ctx: ToolContext): IO[ToolExecResult] = executeTool(call, ctx)

  /** route 调用记录（断言面：llmName 原样、args/sessionId/requestId/agentName 透传）。 */
  final private case class RouteRecord(
    callName: String,
    args: JsonObject,
    sessionId: String,
    requestId: String,
    agentName: String
  )

  /**
   * record-stub 端口：命中集内的名返回固定腿（记调用），集外的名返回 None（模拟
   * route miss——真实现的查表未中面）；exec 不代跑（由闸按腿自行调度）。
   */
  final private class RecordingRoute(hits: Set[String], leg: IrRouteLeg) extends IrRoutePort:
    val calls = new ConcurrentLinkedQueue[RouteRecord]()

    def route(
      call: ToolCall,
      sessionId: String,
      requestId: String,
      agentName: String,
      exec: IrExec
    ): Option[IO[IrRouteLeg]] =
      if !hits.contains(call.name) then None
      else
        calls.add(RouteRecord(call.name, call.input, sessionId, requestId, agentName))
        Some(IO.pure(leg))

  /** 计数引擎：beforeTool/afterTool 每次 emit +1（executeToolInner 真实执行的精确信号）。 */
  final private class CountingHooks(counter: AtomicInteger) extends HookEngine:

    def emit(
      event: HookEvent,
      payload: HookPayload,
      context: HookContext,
      toolName: Option[String] = None
    ): IO[HookResult] =
      IO.delay(counter.incrementAndGet()).as(HookResult())

  private def mkRoot(): os.Path = os.Path(JFiles.createTempDirectory("nb-ir-llm-gate").toString)

  private def mkResources(irRoute: Option[IrRoutePort]): SharedResources =
    val tmp = mkRoot()
    SpecResources
      .mkResources(null, tmp, new StubLlm().handle)
      .map(_.copy(irRoute = irRoute))
      .unsafeRunSync()

  private def mkCtx(
    res: SharedResources,
    hooks: HookEngine,
    requestId: Option[String] = Some("req-gate")
  ): ToolContext =
    ToolContext(
      projectRoot = "",
      sharedResources = Some(res),
      hookEngine = hooks,
      sessionId = Some("sess-gate"),
      requestId = requestId,
      agentDef = Some(nebflow.actor.AgentDef(name = "gate-agent", description = "test"))
    )

  private def readCall(root: os.Path, file: String = "gate.txt"): ToolCall =
    os.write(root / file, "gate line one")
    ToolCall(id = "t1", name = "Read", input = JsonObject("file_path" -> (root / file).toString.asJson))

  test("off 态（irRoute=None 缺省）：executeTool 与现状逐字一致——真 Read，结果不经 IR"):
    val root = mkRoot()
    val call = readCall(root)
    val res = mkResources(None)
    val execs = new AtomicInteger(0)
    val r = CoreProbe.exec(call, mkCtx(res, CountingHooks(execs))).unsafeRunSync()
    assertEquals(r.isError, false, s"Read failed: ${r.content}")
    assert(r.content.startsWith("1\t"), s"cat -n form expected: ${r.content.take(40)}")
    assertEquals(execs.get, 2) // 旧路径真实执行恰一次（beforeTool+afterTool 两次 emit）

  test("on 态 + Executed：结果=stub 产物逐字返回、零真实执行、stub 收 llmName 原样与四元组"):
    val root = mkRoot()
    val call = readCall(root)
    val stub = RecordingRoute(Set("Read"), IrRouteLeg.Executed(ToolExecResult("from-stub")))
    val res = mkResources(Some(stub))
    val execs = new AtomicInteger(0)
    val r = CoreProbe.exec(call, mkCtx(res, CountingHooks(execs))).unsafeRunSync()
    assertEquals(r.content, "from-stub")
    assertEquals(r.isError, false)
    assertEquals(execs.get, 0) // IR 面未触活闭包（hook 面零触发）
    val rec = stub.calls.asScala.toList
    assertEquals(rec.length, 1)
    assertEquals(rec.head.callName, "Read") // 模型面名 = llmName 原样（边界钉）
    assertEquals(rec.head.args("file_path").flatMap(_.asString), Some((root / "gate.txt").toString))
    assertEquals(rec.head.sessionId, "sess-gate")
    assertEquals(rec.head.requestId, "req-gate")
    assertEquals(rec.head.agentName, "gate-agent")

  test("on 态 + route miss（AskUserQuestion / MCP 名）：stub 零调用 ⇒ 旧路径直行"):
    val stub = RecordingRoute(Set.empty, IrRouteLeg.Executed(ToolExecResult("should-not-appear")))
    val res = mkResources(Some(stub))
    val execs = new AtomicInteger(0)
    val ctx = mkCtx(res, CountingHooks(execs))
    val ask = ToolCall(id = "t2", name = "AskUserQuestion", input = JsonObject("question" -> "q".asJson))
    val r1 = CoreProbe.exec(ask, ctx).unsafeRunSync()
    assert(r1.content.nonEmpty) // 真实 executeToolInner 直行（工具自答其无 actor 面）
    assertEquals(execs.get > 0, true)
    val mcp = ToolCall(id = "t3", name = "mcp__srv__tool", input = JsonObject.empty)
    val r2 = CoreProbe.exec(mcp, ctx).unsafeRunSync()
    assertEquals(r2.isError, true) // 不在 TOOL_MAP ⇒ no such tool
    assertEquals(stub.calls.asScala.toList.length, 0) // 两名都未中改道表

  test("畸形参数优先于 IR 闸：rawArguments 报错不被 IR schema 报错遮蔽（Issue #18）"):
    val stub = RecordingRoute(Set.empty, IrRouteLeg.Executed(ToolExecResult("should-not-appear")))
    val res = mkResources(Some(stub))
    val call = ToolCall(
      id = "t4",
      name = "Read",
      input = JsonObject(nebflow.llm.providers.ToolInputJson.RawArgsKey -> "{\"bad\": unquoted}".asJson)
    )
    val r = CoreProbe.exec(call, mkCtx(res, HookEngine.noop)).unsafeRunSync()
    assertEquals(r.isError, true)
    assert(r.content.contains("could not be parsed"), r.content.take(120))
    assertEquals(stub.calls.asScala.toList.length, 0) // 闸先于 IR：畸形腿零 route 咨询

  test("kimi $web_search echo 优先于 IR 闸：stub 零调用、内容=kimiEchoContent"):
    val stub = RecordingRoute(Set.empty, IrRouteLeg.Executed(ToolExecResult("should-not-appear")))
    val res = mkResources(Some(stub))
    val call = ToolCall(id = "t5", name = "$web_search", input = JsonObject("query" -> "q".asJson))
    val r = CoreProbe.exec(call, mkCtx(res, HookEngine.noop)).unsafeRunSync()
    assertEquals(r.isError, false)
    assertEquals(r.content, nebflow.llm.SearchProviderResolver.kimiEchoContent(call))
    assertEquals(stub.calls.asScala.toList.length, 0)

  test("AskFallback：绝不 await_approval——直调 executeToolInner 恰执行一次（计数=1 轮工具执行）"):
    val root = mkRoot()
    val call = readCall(root)
    val stub = RecordingRoute(Set("Read"), IrRouteLeg.AskFallback)
    val res = mkResources(Some(stub))
    val execs = new AtomicInteger(0)
    val r = CoreProbe.exec(call, mkCtx(res, CountingHooks(execs))).unsafeRunSync()
    // 回退腿真实执行：结果是真 Read 输出（非 stub 产物、非错误卡等待）
    assertEquals(r.isError, false, s"fallback execution failed: ${r.content}")
    assert(r.content.startsWith("1\t"), s"real Read expected: ${r.content.take(40)}")
    // 恰一次执行：beforeTool+afterTool 两次 emit（两次=一次执行；四次=异常双跑）
    assertEquals(execs.get, 2)
    assertEquals(stub.calls.asScala.toList.length, 1)

  test("Blocked：Deny/Invalid 硬底——零执行、错误结果原样返回"):
    val root = mkRoot()
    val call = readCall(root)
    val stub = RecordingRoute(
      Set("Read"),
      IrRouteLeg.Blocked(ToolExecResult("[ir rejected] policy.denied rule=x: no", isError = true))
    )
    val res = mkResources(Some(stub))
    val execs = new AtomicInteger(0)
    val r = CoreProbe.exec(call, mkCtx(res, CountingHooks(execs))).unsafeRunSync()
    assertEquals(r.isError, true)
    assert(r.content.contains("policy.denied"), r.content)
    assertEquals(execs.get, 0) // 零执行（连 beforeTool 都不触发）
    assertEquals(stub.calls.asScala.toList.length, 1)

  test("requestId 缺省兜底：ctx.requestId 空 ⇒ llm-<toolCallId>（确定性关联）"):
    val root = mkRoot()
    val call = readCall(root)
    val stub = RecordingRoute(Set("Read"), IrRouteLeg.Executed(ToolExecResult("ok")))
    val res = mkResources(Some(stub))
    CoreProbe.exec(call, mkCtx(res, HookEngine.noop, requestId = None)).unsafeRunSync()
    assertEquals(stub.calls.asScala.toList.head.requestId, "llm-t1")

end IrLlmGateSpec
