/* 命令 IR 路由层 P1-3（LLM ingress 改道）：agent 引擎面 ⇄ IR 路由层的**装配端口**。
 *
 * 层序裁定（先例 core/AgentRuntimePort 的机制镜像）：本文件声明在 nebflow.ir（签名
 * 零 core/agent/gateway 符号——ToolCall/ToolExecResult 在 shared/contracts.scala，
 * PlanResult/Tenant/Status 在本包），core（AgentRuntimePort.irRoute 成员）与 agent
 * （SharedResources.irRoute 尾参，缺省 None=off）向下引用合法；唯一活实现在 gateway
 * （IrLlmRoute，GatewayMain 装配）。开关形状与 hookEngine=noop / hotRestart=None 同形：
 * None ⇒ agent 引擎逐字旧路径（结构性直行，非行为等价）。
 *
 * 双闸消解（钉死）：本端口的唯一调用点 = agent 的 executeTool 第三前置改道闸（kimi
 * echo / 畸形参数之后）——三处 executeTool 调用点全部处于旧 permissionDecision 放行
 * **之后**，故 IR 面只可能收紧不放宽；IR 三态里 Ask 不发卡（回退旧直呼）、Deny/Invalid
 * 只产错误结果 ⇒ 同一调用最多被问一次（旧卡是唯一询问权威）。
 */
package nebflow.ir

import cats.effect.IO
import io.circe.syntax.*
import nebflow.shared.{ToolCall, ToolExecResult}

/**
 * 旧引擎的**完整执行闭包**（P1-3 活闭包裁定）：实现 = 调用方（executeTool 闸）手里的
 * `executeToolInner(call, ctx)`——内含 hook before/after、会话沙箱、fileHistory/
 * readTracker、wsSend、Card modelFacingResult、Edit/Write 摘要与 extractImages。
 * 静态桥模板（projectRoot=""/sandbox=off）**不可**作为 LLM 面执行腿出货；经本闭包，
 * 上述纵深全部经旧引擎保留（gateway IrToolBridge 的 fiber-local 分支消费本口）。
 */
trait IrExec:
  def run(): IO[ToolExecResult]
end IrExec

/**
 * 改道腿三态（ingress=llm 策略缺省，绝不 await_approval）：
 *  - [[Executed]]：IR 全 Allow 执行完成（Router 状态 Done）——结果以 IR 契约为权威
 *    （isError = exit≠0；content = final stdout / 失败语义串），富字段
 *    （frontendContent/imageBlocks）由活闭包旁路保真；
 *  - [[AskFallback]]：IR 判 Ask ⇒ **回退旧直呼路径**（§8.4 步 5b 对 LLM 租户不实现——
 *    按字面实现 await_approval 会卡死轮次：agent 等 tool_result 而模型被 [L3] 限单 Call
 *    且无凭据重提交能力；Router.audit 已对该节点落 decision=ask/exit=null 审计留痕，
 *    回退腿不另造审计面）；
 *  - [[Blocked]]：IR Deny（硬底零执行零回退）或 Invalid（[L2] 校验禁止跳过）/防御性
 *    兜底态——isError=true、content 携错误码与规则/路径供模型自纠。
 */
enum IrRouteLeg:
  case Executed(result: ToolExecResult)
  case AskFallback
  case Blocked(result: ToolExecResult)
end IrRouteLeg

/**
 * LLM 工具调用改道端口：`None` = llmName 未中改道表（或 IR 表未就绪）⇒ 调用方走旧路径
 * 逐字直行；`Some(io)` = 改道腿（io 求值后给出三态之一）。签名只携带 shared/ir 符号。
 */
trait IrRoutePort:

  def route(
    call: ToolCall,
    sessionId: String,
    requestId: String,
    agentName: String,
    exec: IrExec
  ): Option[IO[IrRouteLeg]]
end IrRoutePort

/**
 * PlanResult → ToolExecResult 的**纯映射**（表驱动可测；只吃 PlanResult，不吃闭包）。
 *
 * 契约新做（P1-3 侦察钉死）：旧引擎的 isError 判定是**路径分道**（Left/hook Block/异常/
 * 权限拒），不是 exit code；IR 面的 isError **必须**新做 `exit≠0 ⇒ isError=true`。
 *
 * imageBlocks 降级基线（显式承认）：IR 线契约 StreamValue=Text|Jsonl 结构性载不了图像，
 * 「Read 图像经 IR 退化为文本」在 PlanResult 边界为真 ⇒ 纯映射的 imageBlocks 恒 None
 * （测试钉死）；活路径经 IrExec 闭包把 frontendContent/imageBlocks 富字段旁路带回
 * （见 gateway IrLlmRoute 的旁路保真合并），不经过本映射。
 */
object IrLlmContract:

  /** 计划级结果 → 改道腿三态（Done⇒Executed；AwaitApproval⇒AskFallback；其余⇒Blocked）。 */
  def legOf(plan: PlanResult): IrRouteLeg = plan.status match
    case Status.Done => IrRouteLeg.Executed(done(plan))
    case Status.AwaitApproval => IrRouteLeg.AskFallback
    case _ => IrRouteLeg.Blocked(blocked(plan))

  /** Done：content=final stdout（成功）/错误语义串（失败）；isError=exit≠0（契约新做）。 */
  def done(plan: PlanResult): ToolExecResult =
    val failed = plan.exit.exists(_ != ExitCode.Ok)
    if failed then
      ToolExecResult(
        content = plan.error.fold(s"ir command failed (exit=${plan.exit.getOrElse("?")})")(renderError),
        isError = true,
        frontendContent = None,
        imageBlocks = None
      )
    else
      val text = plan.finalStdout.map(_.asText).getOrElse("")
      ToolExecResult(
        content = text,
        isError = false,
        frontendContent = Some(text),
        imageBlocks = None
      )
    end if
  end done

  /** Rejected/Invalid/Cancelled 等一切非 Done 面：isError=true，content 携码与细节供模型自纠。 */
  def blocked(plan: PlanResult): ToolExecResult =
    val exitText = plan.exit.fold("null")(_.toString)
    val body = plan.error.fold("no error detail")(renderError)
    ToolExecResult(
      content = s"[ir ${Status.wire(plan.status)} exit=$exitText] $body",
      isError = true,
      frontendContent = None,
      imageBlocks = None
    )
  end blocked

  /** 错误对象 → 模型可读串：`code rule=… paths=[…]: message`（code 稳定、message 辅读）。 */
  def renderError(err: IrError): String =
    val rule = err.details("rule").flatMap(_.asString).fold("")(r => s" rule=$r")
    val paths = err
      .details("paths")
      .flatMap(_.asArray)
      .filter(_.nonEmpty)
      .map(_.flatMap(_.asString).mkString(", "))
      .fold("")(p => s" paths=[$p]")
    s"${err.code}$rule$paths: ${err.message}"
  end renderError

end IrLlmContract
