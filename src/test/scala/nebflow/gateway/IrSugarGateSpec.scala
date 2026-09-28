package nebflow.gateway

import io.circe.Json
import io.circe.syntax.*
import munit.FunSuite

import java.io.File
import scala.io.Source

/**
 * 人侧 argv 糖分流门（P1-1）——行为面 + 源码钉串门（UserTextGateSpec 风格）。
 *
 * 依据（`.zcode/plans/command-ir-standard.md` §9:547）：前端斜杠闸（`/clear`、`/compact`
 * 等 SLASH_ALLOWED 通道）**优先消费**，未被消费的 `/…` 才进 router lowering，**禁止两处
 * 解析同一输入**。服务端分流闸 = `WebSocketRoutes.handleMessage` 里、msgType 取值之后、
 * `WsDispatch.handlers` 派发之前的单点判定（`WsIrHandlers.shouldLowerToSugar`）。
 *
 * 覆盖边界（🔴 逐字）：① 行为读数只覆盖**纯函数面**（shouldLowerToSugar 真值表——不起
 * gateway 栈）；糖腿的端到端行为读数归 `scripts/smoke-ir-sugar.mjs`（隔离实例实测），
 * 本批禁起服务 ⇒ **未跑 ≠ 已绿**。② 源码钉串只认**代码面**（codeOnly，先例
 * UserTextGateSpec）：注释里提到符号不算。③ 负控只扫 `src/main/scala`（后端）。
 */
class IrSugarGateSpec extends FunSuite:

  private val repoRoot = new java.io.File(".").getAbsoluteFile

  private val wsRoutesFile =
    new java.io.File(repoRoot, "src/main/scala/nebflow/gateway/WebSocketRoutes.scala")

  private val wsIrHandlersFile =
    new java.io.File(repoRoot, "src/main/scala/nebflow/gateway/WsIrHandlers.scala")

  private val wsSessionChatFile =
    new java.io.File(repoRoot, "src/main/scala/nebflow/gateway/WsSessionChatHandlers.scala")
  private val mainScalaDir = new java.io.File(repoRoot, "src/main/scala")

  private def read(f: java.io.File): String =
    assert(f.isFile, s"源码不存在：${f.getPath}（本 spec 必须在仓根运行）")
    val src = Source.fromFile(f, "UTF-8")
    try src.mkString
    finally src.close()

  /** 剥块注释与行注释后的代码面（判据只认代码，先例 UserTextGateSpec.codeOnly）。 */
  private def codeOnly(src: String): String =
    val noBlock = src.replaceAll("(?s)/\\*.*?\\*/", "")
    noBlock.linesIterator.map(l => l.replaceAll("//.*$", "")).mkString("\n")

  private lazy val wsRoutesCode: String = codeOnly(read(wsRoutesFile))
  private lazy val wsIrHandlersCode: String = codeOnly(read(wsIrHandlersFile))
  private lazy val wsSessionChatCode: String = codeOnly(read(wsSessionChatFile))

  private def scalaSourcesUnder(dir: java.io.File): List[java.io.File] =
    val out = scala.collection.mutable.ListBuffer.empty[java.io.File]
    def go(d: java.io.File): Unit =
      val kids = d.listFiles()
      if kids != null then
        kids.foreach { k =>
          if k.isDirectory then go(k)
          else if k.getName.endsWith(".scala") then out += k
        }
    go(dir)
    out.toList

  // ---------------------------------------------------------------
  // ① 行为面：shouldLowerToSugar 真值表（纯函数，不起栈）
  // ---------------------------------------------------------------

  private def frame(content: String, extra: (String, Json)*): Json =
    Json.obj(("content" -> content.asJson) +: extra*)

  test("① 行为：三腿（typeless/immediateInput/userMessage）× 人侧糖形态 ⇒ lower"):
    for leg <- List("", "immediateInput", "userMessage") do
      assertEquals(WsIrHandlers.shouldLowerToSugar(leg, frame("/dev:fs:ls")), true, s"leg='$leg'")
      assertEquals(WsIrHandlers.shouldLowerToSugar(leg, frame("/dev:fs:ls --path .")), true, s"leg='$leg'")

  test("① 行为：非三腿 msgType 一律不 lower（command/askUserAnswer/ir/getHistory…）"):
    for leg <- List("command", "askUserAnswer", "permissionAnswer", "ir", "getHistory", "interrupt") do
      assertEquals(WsIrHandlers.shouldLowerToSugar(leg, frame("/dev:fs:ls")), false, s"leg='$leg'")

  test("① 行为：保留名 / // 转义 / 单段 / 大小写 / 空 content ⇒ 不 lower（§9:547）"):
    for content <- List(
        "/clear",
        "/compact",
        "/ask",
        "/onboarding",
        "/skill:x",
        "//dev:fs:ls",
        "/dev",
        "/devfoo:x",
        "/DEV:fs:ls",
        "",
        "   ",
        "hello"
      )
    do assertEquals(WsIrHandlers.shouldLowerToSugar("userMessage", frame(content)), false, s"content='$content'")

  test("① 行为：附件/引用帧不 lower（fail-open——糖是纯文本特性）；空数组仍可 lower"):
    assertEquals(WsIrHandlers.shouldLowerToSugar("", frame("/dev:fs:ls", "attachments" -> Json.arr(Json.obj()))), false)
    assertEquals(WsIrHandlers.shouldLowerToSugar("", frame("/dev:fs:ls", "refs" -> Json.arr("r".asJson))), false)
    assertEquals(WsIrHandlers.shouldLowerToSugar("", frame("/dev:fs:ls", "taskRefs" -> Json.arr("t".asJson))), false)
    // 键在场但为空 ⇒ 纯文本帧，照常 lower
    assertEquals(WsIrHandlers.shouldLowerToSugar("", frame("/dev:fs:ls", "attachments" -> Json.arr())), true)
    assertEquals(WsIrHandlers.shouldLowerToSugar("", frame("/dev:fs:ls", "refs" -> Json.arr())), true)
    // 非数组非 null 的载荷值 ⇒ 视为非空（fail-open，不 lower）
    assertEquals(WsIrHandlers.shouldLowerToSugar("", frame("/dev:fs:ls", "refs" -> Json.obj())), false)

  // ---------------------------------------------------------------
  // ② 源码钉串：接入点唯一 + 准入不降低（codeOnly）
  // ---------------------------------------------------------------

  test("② 钉串：handleMessage 体含分流闸，且早于 WsDispatch.handlers.get(msgType) 派发"):
    val start = wsRoutesCode.indexOf("private def handleMessage")
    val end = wsRoutesCode.indexOf("end handleMessage", start)
    assert(start >= 0 && end > start, "handleMessage 定义体定位失败（签名/收尾被改名或搬迁）")
    val body = wsRoutesCode.substring(start, end)
    val gateIdx = body.indexOf("WsIrHandlers.shouldLowerToSugar")
    val dispatchIdx = body.indexOf("WsDispatch.handlers.get(msgType)")
    assert(gateIdx >= 0, "handleMessage 不含 WsIrHandlers.shouldLowerToSugar —— 分流闸丢失")
    assert(dispatchIdx > gateIdx, "分流闸必须早于 WsDispatch.handlers.get(msgType)（既有闸优先消费）")

  test("② 钉串：糖分支体先过 rateLimiter.check(\"ws\")（与 typeless 腿同 limiter 同 key）"):
    val start = wsRoutesCode.indexOf("private def handleMessage")
    val end = wsRoutesCode.indexOf("end handleMessage", start)
    val body = wsRoutesCode.substring(start, end)
    val gateIdx = body.indexOf("WsIrHandlers.shouldLowerToSugar")
    val rateIdx = body.indexOf("rateLimiter.check(\"ws\")")
    assert(rateIdx > gateIdx, "糖分支必须复用 rateLimiter.check(\"ws\")，不得绕过限流")

  test("② 钉串：WsIrHandlers 代码面含 admitWorkOrRefuse / ir.sugar / irResult / ArgvSugar.lower"):
    for sym <- List("admitWorkOrRefuse", "Some(\"ir.sugar\")", "\"irResult\"", "ArgvSugar.lower") do
      assert(wsIrHandlersCode.contains(sym), s"WsIrHandlers 代码面缺 $sym —— 糖腿成形出口/准入被改坏")

  // ---------------------------------------------------------------
  // ③ 负控：单处消费（同一输入两处解析在结构上不可达）
  // ---------------------------------------------------------------

  test("③ 负控：main 源树 ArgvSugar.lower/humanShape 调用点仅 WsIrHandlers 一处"):
    val offenders = scalaSourcesUnder(mainScalaDir).flatMap { f =>
      val code = codeOnly(read(f))
      val hits = List("ArgvSugar.lower", "ArgvSugar.humanShape").filter(code.contains)
      // ArgvSugar.scala 自身是定义点（def lower / def humanShape），不会以 `ArgvSugar.` 前缀出现。
      // 路径统一 '/' 分隔（Windows 反斜杠不进断言面）。
      if hits.isEmpty then Nil
      else
        List(s"${f.getPath.stripPrefix(repoRoot.getPath + File.separator).replace('\\', '/')}: ${hits.mkString(", ")}")
    }
    assertEquals(
      offenders,
      List("src/main/scala/nebflow/gateway/WsIrHandlers.scala: ArgvSugar.lower, ArgvSugar.humanShape"),
      s"lowering 调用点必须唯一（越界即停）:\n  ${offenders.mkString("\n  ")}"
    )

  // ---------------------------------------------------------------
  // ④ 正控：既有三腿消息通道不被糖腿波及（防越界删除）
  // ---------------------------------------------------------------

  test("④ 正控：handleUserText / dispatchUserText / ImmediateInput 投放钉串仍在"):
    assert(wsRoutesCode.contains("private def handleUserText"), "handleUserText 丢失（越界删除）")
    assert(wsRoutesCode.contains("private def dispatchUserText"), "dispatchUserText 丢失（越界删除）")
    assert(
      wsRoutesCode.contains("AgentCommand.ImmediateInput(contentForAgent, fromUser = fromUser)"),
      "dispatchUserText 不再投 ImmediateInput —— 普通消息通道被改坏"
    )

  test("④ 正控：handleCommand 的 slash.clearDone 气泡先例未被波及"):
    assert(wsSessionChatCode.contains("Some(\"slash.clearDone\")"), "slash.clearDone 系统气泡先例丢失")

end IrSugarGateSpec
