package nebflow.ir

import io.circe.syntax.*
import munit.FunSuite

/**
 * P1-3 契约映射的表驱动用例：PlanResult × {Done exit0 / Done exit≠0 / Rejected /
 * Invalid / AwaitApproval / Cancelled} → ToolExecResult 逐字段断言 + 改道腿三态 +
 * imageBlocks=None 降级基线钉死（纯映射只吃 PlanResult——「Read 图像经 IR 退化为文本」
 * 在该边界为真；活路径的富字段旁路保真由 IrLlmRouteSpec 的活路径腿覆盖）。
 *
 * 契约要点（P1-3 侦察钉死）：旧引擎的 isError 判定是**路径分道**不是 exit code ⇒
 * IR 面必须新做 `exit≠0 ⇒ isError=true`；frontendContent 基线 = Some(stdout.asText)
 * （失败腿 stdout 缺席 ⇒ None——ToolEnd 的 fallback 是 content，Some("") 无消费者）；
 * Blocked 面 content 携 error.code + rule/paths 供模型自纠。
 */
class IrLlmContractSpec extends FunSuite:

  private val done0: PlanResult = PlanResult(
    requestId = "r-1",
    status = Status.Done,
    exit = Some(0),
    results = List(NodeResult("0", "dev:tool:read", 0, Some(StreamValue.Text("hello world")))),
    finalStdout = Some(StreamValue.Text("hello world"))
  )

  private val doneFail: PlanResult = PlanResult(
    requestId = "r-2",
    status = Status.Done,
    exit = Some(1),
    results = List(NodeResult("0", "dev:tool:read", 1, None, error = Some(IrError.commandFailed("boom-msg")))),
    finalStdout = None,
    error = Some(IrError.commandFailed("boom-msg"))
  )

  private val rejected: PlanResult = PlanResult(
    requestId = "r-3",
    status = Status.Rejected,
    exit = Some(125),
    error = Some(IrError.policyDenied("not allowed here", "test:deny", "0"))
  )

  private val invalid: PlanResult = PlanResult(
    requestId = "r-4",
    status = Status.Invalid,
    exit = Some(2),
    error = Some(IrError.invalidArgs("missing required param 'x'", List(".x")))
  )

  private val awaitApproval: PlanResult = PlanResult(
    requestId = "r-5",
    status = Status.AwaitApproval,
    exit = None,
    approval = Some(ApprovalRequest("pd", "cd", Nil, "approval required", "bridge:opaque-host"))
  )

  private val cancelled: PlanResult = PlanResult(
    requestId = "r-6",
    status = Status.Cancelled,
    exit = Some(130)
  )

  test("腿三态：Done⇒Executed；AwaitApproval⇒AskFallback；Rejected/Invalid/Cancelled⇒Blocked"):
    assertEquals(IrLlmContract.legOf(done0).isInstanceOf[IrRouteLeg.Executed], true)
    assertEquals(IrLlmContract.legOf(awaitApproval), IrRouteLeg.AskFallback)
    assertEquals(IrLlmContract.legOf(rejected).isInstanceOf[IrRouteLeg.Blocked], true)
    assertEquals(IrLlmContract.legOf(invalid).isInstanceOf[IrRouteLeg.Blocked], true)
    assertEquals(IrLlmContract.legOf(cancelled).isInstanceOf[IrRouteLeg.Blocked], true)

  test("Done exit=0：content=final stdout、isError=false、frontendContent=Some(text)、imageBlocks=None"):
    val r = IrLlmContract.done(done0)
    assertEquals(r.content, "hello world")
    assertEquals(r.isError, false)
    assertEquals(r.frontendContent, Some("hello world"))
    assertEquals(r.imageBlocks, None)

  test("Done exit≠0：isError=true（契约新做——isError 原是路径分道非 exit code）、content=错误语义串、imageBlocks=None"):
    val r = IrLlmContract.done(doneFail)
    assertEquals(r.isError, true)
    assertEquals(r.content, "command.failed: boom-msg")
    assertEquals(r.frontendContent, None)
    assertEquals(r.imageBlocks, None)

  test("Rejected(Deny)：isError=true、content 携 policy.denied 与 rule"):
    val r = IrLlmContract.blocked(rejected)
    assertEquals(r.isError, true)
    assert(r.content.contains("policy.denied"), r.content)
    assert(r.content.contains("rule=test:deny"), r.content)

  test("Invalid(exit 2)：content 携 code 与 details 逐路径（[L2] 校验禁止跳过——供模型自纠）"):
    val r = IrLlmContract.blocked(invalid)
    assertEquals(r.isError, true)
    assert(r.content.contains("router.invalid_args"), r.content)
    assert(r.content.contains("paths=[.x]"), r.content)

  test("Cancelled 等防御性态 ⇒ Blocked fail-closed（Router 现不产出，映射兜底）"):
    val r = IrLlmContract.blocked(cancelled)
    assertEquals(r.isError, true)
    assert(r.content.contains("cancelled"), r.content)
    assert(r.content.contains("exit=130"), r.content)

  test("降级基线钉死：六形态纯映射 imageBlocks 恒 None（StreamValue=Text|Jsonl 结构性载不了图像）"):
    List(done0, doneFail, rejected, invalid, awaitApproval, cancelled).foreach { p =>
      IrLlmContract.legOf(p) match
        case IrRouteLeg.Executed(r) => assertEquals(r.imageBlocks, None)
        case IrRouteLeg.Blocked(r) => assertEquals(r.imageBlocks, None)
        case IrRouteLeg.AskFallback => ()
    }

  test("renderError：code 稳定在前、rule/paths 只在在场时拼接"):
    assertEquals(IrLlmContract.renderError(IrError.commandFailed("x")), "command.failed: x")
    assertEquals(
      IrLlmContract.renderError(IrError.invalidArgs("bad", List(".a", ".b"))),
      "router.invalid_args paths=[.a, .b]: bad"
    )

end IrLlmContractSpec
