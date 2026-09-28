package nebflow.ir

import io.circe.syntax.*
import io.circe.{Json, JsonObject}

/**
 * 错误码、退出码、节点结果与信封（标准 §6）。
 *
 * 三条不变量（[I6] / §6.1 / §6.2）：
 *  - 退出码是**值**不是异常；路由层自身故障**禁止**伪装成退出码（走 `status=invalid`）。
 *  - `code` 是稳定机器契约（点分层级），`message` 不保证稳定（禁止被程序消费）。
 *  - **单一 code 原则**（[D32]）：schema 类违规统一 `router.invalid_args`，
 *    不为同一语义分叉多个 code（消费者会分叉判断）。
 */
object Codes:

  // policy.*（§8）
  val PolicyDenied = "policy.denied"
  val PolicyUntrusted = "policy.untrusted"
  val PolicyApprovalRejected = "policy.approval_rejected"

  // router.schema.*（§3.3 / §10.1）
  val SchemaNodeTag = "router.schema.node_tag"
  val SchemaUnknownKey = "router.schema.unknown_key"
  val SchemaBadValue = "router.schema.bad_value"
  val SchemaNameInvalid = "router.schema.name_invalid"
  val SchemaBadVersion = "router.schema.bad_version"

  // router.stream.*（§4.3 / §4.4 / §5.3）
  val StreamMismatch = "router.stream.mismatch"
  val StreamViolation = "router.stream.violation"
  val StreamDecode = "router.stream.decode"

  // router.limit.*（§3.3 R9 / §5.2）
  val LimitNodes = "router.limit.nodes"
  val LimitDepth = "router.limit.depth"
  val LimitOutput = "router.limit.output"

  // router.*（§6.1）
  val UnknownCommand = "router.unknown_command"
  val InvalidArgs = "router.invalid_args"
  val Timeout = "router.timeout"
  val Cancelled = "router.cancelled"
  val BindingUnavailable = "router.binding_unavailable"
  val Internal = "router.internal"

  // command.*（§6.2）
  val CommandFailed = "command.failed"

  /** 审计 `capsSource` 三值（§8.6）：声称有 / 引擎不认识 / 由 Redirect 计算。 */
  val CapsDeclared = "declared"
  val CapsPolicyUnknown = "policy-unknown"
  val CapsRedirect = "redirect"

end Codes

/** 退出码表（§6.1，对齐 bash/coreutils 习惯，人类与 LLM 两读）。 */
object ExitCode:
  val Ok = 0
  val Failure = 1
  val Usage = 2

  /** 超时（对齐 coreutils `timeout`）：执行过且被中断。 */
  val Timeout = 124

  /** 路由层拒绝（策略 Deny / 未信任 / 审批被拒 / 计划含 Deny）：**未执行**。 */
  val Rejected = 125

  /** 不可执行（binding 不可用）：**未执行**。 */
  val NotExecutable = 126

  /** 未知命令：**未执行**。 */
  val UnknownCommand = 127

  /** 取消 / 中断（对齐 SIGINT 惯例）。 */
  val Cancelled = 130

  /** 计划非法时按首个致因取码（§6.4）：node_tag/unknown_key/… 类 shape 违规 = 用法错误。 */
  val planInvalidDefault = Usage
end ExitCode

/** 信封状态（§6.5）。 */
enum Status:
  case Done, AwaitApproval, Rejected, Cancelled, Invalid

object Status:

  def wire(s: Status): String = s match
    case Done => "done"
    case AwaitApproval => "await_approval"
    case Rejected => "rejected"
    case Cancelled => "cancelled"
    case Invalid => "invalid"

end Status

/** 错误对象（§6.2）：`code` 稳定、`message` 不稳定、`details` 逐条列违规路径。 */
final case class IrError(
  code: String,
  message: String,
  details: JsonObject = JsonObject.empty,
  retryable: Boolean = false
):

  def toJson: Json =
    Json.obj(
      "code" -> code.asJson,
      "message" -> message.asJson,
      "details" -> Json.fromJsonObject(details),
      "retryable" -> retryable.asJson
    )

  def withDetail(key: String, value: Json): IrError =
    copy(details = details.add(key, value))

end IrError

object IrError:

  def nodeTag(message: String): IrError = IrError(Codes.SchemaNodeTag, message)

  def unknownKey(key: String): IrError =
    IrError(Codes.SchemaUnknownKey, s"unknown key '$key'").withDetail("key", key.asJson)

  def badValue(message: String): IrError = IrError(Codes.SchemaBadValue, message)

  def nameInvalid(name: String, reason: String): IrError =
    IrError(Codes.SchemaNameInvalid, s"invalid command name '$name': $reason")
      .withDetail("name", name.asJson)
      .withDetail("reason", reason.asJson)

  def badVersion(got: Int, supported: Int): IrError =
    IrError(Codes.SchemaBadVersion, s"unsupported ir version $got (supported: $supported)")
      .withDetail("got", got.asJson)
      .withDetail("supported", supported.asJson)

  def invalidArgs(message: String, paths: List[String] = Nil): IrError =
    IrError(Codes.InvalidArgs, message).withDetail("paths", paths.asJson)

  def stdoutLimit(kind: String, produced: Long, limit: Long): IrError =
    IrError(Codes.LimitOutput, s"stdout limit exceeded for $kind stream (no truncation — see §5.2)")
      .withDetail("kind", kind.asJson)
      .withDetail("produced", produced.asJson)
      .withDetail("limit", limit.asJson)

  def commandFailed(message: String): IrError = IrError(Codes.CommandFailed, message)

  def unknownCommand(name: String): IrError =
    IrError(Codes.UnknownCommand, s"unknown command '$name'").withDetail("name", name.asJson)

  def bindingUnavailable(name: String, reason: String): IrError =
    IrError(Codes.BindingUnavailable, s"binding unavailable for '$name': $reason")
      .withDetail("name", name.asJson)
      .withDetail("reason", reason.asJson)

  def policyDenied(reason: String, rule: String, node: String): IrError =
    IrError(Codes.PolicyDenied, reason).withDetail("rule", rule.asJson).withDetail("node", node.asJson)

  def policyUntrusted(name: String): IrError =
    IrError(Codes.PolicyUntrusted, s"command '$name' belongs to an untrusted workspace").withDetail(
      "name",
      name.asJson
    )

  def internal(message: String): IrError = IrError(Codes.Internal, message)

end IrError

/** 管道上的流值（§6.3 的单键标签自描述形态）。 */
enum StreamValue:
  case Text(value: String)
  case Jsonl(items: List[Json])

  def toJson: Json = this match
    case Text(v) => Json.obj("text" -> v.asJson)
    case Jsonl(items) => Json.obj("jsonl" -> Json.fromValues(items))

  /** 按 §5.3 的**唯一允许强转** `jsonl→text`：逐值走单点打印器 + `\n`。 */
  def asText: String = this match
    case Text(v) => v
    case Jsonl(items) => items.map(Canonical.print).map(_ + "\n").mkString

  /** 输出规模（限额判定用：text = 字节数，jsonl = 条数）。 */
  def size: Long = this match
    case Text(v) => v.getBytes(java.nio.charset.StandardCharsets.UTF_8).length.toLong
    case Jsonl(items) => items.length.toLong

end StreamValue

/** 单节点结果（§6.3）。`node` = §3.3 的节点路径（根 `"0"`，第 i 子 = `父.i`）。 */
final case class NodeResult(
  node: String,
  command: String,
  exit: Int,
  stdout: Option[StreamValue] = None,
  stderr: String = "",
  error: Option[IrError] = None,
  durationMs: Long = 0L
):

  def toJson: Json =
    Json
      .obj(
        "node" -> node.asJson,
        "command" -> command.asJson,
        "exit" -> exit.asJson,
        "stderr" -> stderr.asJson,
        "error" -> error.map(_.toJson).getOrElse(Json.Null),
        "durationMs" -> durationMs.asJson
      )
      .deepMerge(stdout.map(v => Json.obj("stdout" -> v.toJson)).getOrElse(Json.obj()))

end NodeResult

/** 审批展示的节点条目（§8.4 步 5b / §8.5）。 */
final case class ApprovalNode(node: String, command: String, caps: List[String], argsDigest: String):

  def toJson: Json =
    Json.obj(
      "node" -> node.asJson,
      "command" -> command.asJson,
      "caps" -> caps.asJson,
      "argsDigest" -> argsDigest.asJson
    )

/** 审批请求体（§6.5 的 `approval` 键）。 */
final case class ApprovalRequest(
  planDigest: String,
  capsDigest: String,
  nodes: List[ApprovalNode],
  reason: String,
  rule: String
):

  def toJson: Json =
    Json.obj(
      "planDigest" -> planDigest.asJson,
      "capsDigest" -> capsDigest.asJson,
      "nodes" -> Json.fromValues(nodes.map(_.toJson)),
      "reason" -> reason.asJson,
      "rule" -> rule.asJson
    )

end ApprovalRequest

/** 计划级结果 = 响应信封（§6.5）。 */
final case class PlanResult(
  requestId: String,
  status: Status,
  exit: Option[Int],
  results: List[NodeResult] = Nil,
  finalStdout: Option[StreamValue] = None,
  error: Option[IrError] = None,
  approval: Option[ApprovalRequest] = None
):
  def isDone: Boolean = status == Status.Done

  def toJson: Json =
    Json
      .obj(
        // 版本取叶子常量而非 `Ir.Version`：错误面不该反向依赖 AST 文件（保持包内文件图无环）
        "ir" -> IrWire.Version.asJson,
        "requestId" -> requestId.asJson,
        "status" -> Status.wire(status).asJson,
        "exit" -> exit.map(_.asJson).getOrElse(Json.Null),
        "results" -> Json.fromValues(results.map(_.toJson))
      )
      .deepMerge(
        Json.obj(
          "final" -> finalStdout.map(_.toJson).getOrElse(Json.Null),
          "error" -> error.map(_.toJson).getOrElse(Json.Null),
          "approval" -> approval.map(_.toJson).getOrElse(Json.Null)
        )
      )

end PlanResult
