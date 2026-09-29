/* 命令 IR 路由层 P1-3（LLM ingress 改道）：[[nebflow.ir.IrRoutePort]] 的唯一活实现
 * （private[gateway]；agent 看不见 IrGateway，故经 SharedResources.irRoute 端口注入——
 * 先例 core/AgentRuntimePort 的机制镜像）。
 *
 * 本文件独占四处 agent 看不见的装配事实：
 *  ① llmName → IR 名查表：[[IrToolCaps.bridged]] 按 toolName 反查（单源，不改注册面），
 *     叠加 IrGateway registry 的就绪检查（boot 早期 initBridge 前表空 ⇒ 视同 route
 *     miss ⇒ 旧路径，fail-open 到 off 态，保守）；
 *  ② 信封构造：Tenant.Llm(sessionId, agentName) + Ingress.Llm + [L3] 保证的单
 *     Ir.Call(irName, call.input)——端口结构上只可能构造单 Call（双保险）；
 *  ③ VFS 根：经 gateway 单点 [[ExplorerRoots.resolve]]（[D20]）；
 *  ④ 安全档映射：gateway 单点 [[IrSafety.of]]（[D24]，人侧糖腿共用一份）。
 *
 * ingress=llm 策略缺省（绝不 await_approval）：Done ⇒ Executed（isError=exit≠0 契约
 * 新做，IrLlmContract）；AwaitApproval ⇒ AskFallback（回退旧直呼——Router.audit 已
 * 落 decision=ask 审计留痕，回退腿不另造审计面）；Rejected/Invalid/防御性态 ⇒
 * Blocked fail-closed。限额让位：llm 腿走 [[IrGateway.llmInstance]]（llmLimits）。
 */
package nebflow.gateway

import cats.effect.{IO, Ref}
import io.circe.Json
import nebflow.ir.*
import nebflow.shared.{ToolCall, ToolExecResult}

import scala.concurrent.duration.*

private[gateway] final class IrLlmRoute(
  router: Router,
  lookupIr: String => Option[CommandDef],
  rootFor: String => IO[String],
  safetyOf: IO[nebflow.core.SafetyMode]
) extends IrRoutePort:

  def route(
    call: ToolCall,
    sessionId: String,
    requestId: String,
    agentName: String,
    exec: IrExec
  ): Option[IO[IrRouteLeg]] =
    IrLlmRoute.llmNameToIr.get(call.name).flatMap { irName =>
      // registry 就绪检查：boot 早期（initBridge 之前）def 未注册 ⇒ route miss ⇒ 旧路径
      if lookupIr(irName).isDefined then Some(submit(irName, call, sessionId, requestId, agentName, exec))
      else None
    }
  end route

  private def submit(
    irName: String,
    call: ToolCall,
    sessionId: String,
    requestId: String,
    agentName: String,
    exec: IrExec
  ): IO[IrRouteLeg] =
    // 富字段旁路保真：活闭包的 ToolExecResult（frontendContent/imageBlocks）经本 Ref
    // 随 fiber 带回；isError/content 以 IR 契约为权威（content 与闭包模型面串字节相等
    // ——handler 把闭包结果原样包成 Text/commandFailed）。
    for
      rootStr <- rootFor(sessionId)
      mode <- safetyOf
      rich <- Ref.of[IO, Option[ToolExecResult]](None)
      liveExec: IrExec = () => exec.run().flatMap(r => rich.set(Some(r)).as(r))
      envelope = IrRequest(
        plan = Ir.Call(irName, call.input),
        tenant = Tenant.Llm(sessionId, agentName),
        ingress = Ingress.Llm,
        sessionId = sessionId,
        requestId = requestId
      )
      plan <- IrToolBridge.llmLocally(liveExec)(
        router.submit(envelope, VfsRoot(os.Path(rootStr, os.pwd)), IrSafety.of(mode))
      )
      leg <- IrLlmContract.legOf(plan) match
        case IrRouteLeg.Executed(base) =>
          rich.get.map(richOpt => IrRouteLeg.Executed(IrLlmRoute.bypassEnrich(base, richOpt)))
        case other => IO.pure(other)
    yield leg
  end submit

end IrLlmRoute

private[gateway] object IrLlmRoute:

  /**
   * ① llmName → IR 名单源查表（`IrToolCaps.bridged` 按 toolName 反查；模型面工具名
   * = 旧 Tool 名 = llmName，端到端不被换成 IR 名）。双向唯一由 IrLlmRouteSpec 钉
   * （桥表双射 + 注册期 §7.5 uniqueness 负例）。
   */
  val llmNameToIr: Map[String, String] = IrToolCaps.bridged.map(row => row.toolName -> row.irName).toMap

  /**
   * 限额/超时让位裁定（保守）：llm 视图 IrLimits 放宽 nodeTimeout 与 maxTextBytes，
   * 使 IR 限额**永不抢跑**既有治理——declaredToolTimeoutMs（工具自报授权时长，可长于
   * 60s）、BashResilience（后台硬超时 30min 档）、ToolResultGuard（#38 调校过的上下文
   * 保护单点：50K chars 阈值持久化+预览）。若 IR router.limit.output 先触发，大 Read
   * 会从「持久化+预览（isError=false）」劣化为「硬错误」——回归，不让发生。IR §5.2
   * 限额在此阶段边界内让位（openQuestions 留痕，待作者裁定是否收紧为显式策略条目）。
   */
  val llmLimits: IrLimits = IrLimits.default.copy(
    // > BashBackgroundHardTimeoutMs(30min) 且 > 一切合理 declaredToolTimeoutMs 档
    nodeTimeout = 24.hours,
    // > ToolResultGuard 阈值（≤50K chars × UTF-8 4B）的任何真实工具产出——旧直呼路径
    // 本无此限（guard 在闸后无条件运行），llm 腿不引入新失败面
    maxTextBytes = 2L * 1024 * 1024 * 1024
  )

  /** Executed 腿的富字段合并：isError/content 保持 IR 契约权威，富字段取闭包值优先。 */
  def bypassEnrich(base: ToolExecResult, richOpt: Option[ToolExecResult]): ToolExecResult =
    base.copy(
      frontendContent = richOpt.flatMap(_.frontendContent).orElse(base.frontendContent),
      imageBlocks = richOpt.flatMap(_.imageBlocks).orElse(base.imageBlocks)
    )

  /**
   * 配置键 `nebflow.json` → `ir.llmIngress`（bool，缺省 false=off——保守缺省；
   * 非法/缺失一律 fail-safe 关）。openQuestions：命名与缺省值待作者认可。
   */
  def llmIngressEnabled(node: Option[Json]): Boolean =
    node.flatMap(_.hcursor.downField("llmIngress").as[Boolean].toOption).getOrElse(false)

  /** 生产装配：IrGateway 的 llm 限额视图 Router + registry 查表 + 根/档位单点。 */
  def forGateway(sessionStore: nebflow.core.SessionStore): IrLlmRoute = new IrLlmRoute(
    router = IrGateway.llmInstance,
    lookupIr = IrGateway.lookup,
    rootFor = sid => ExplorerRoots.resolve(sessionStore, sid, None),
    safetyOf = nebflow.core.GlobalSafety.defaultMode
  )

end IrLlmRoute
