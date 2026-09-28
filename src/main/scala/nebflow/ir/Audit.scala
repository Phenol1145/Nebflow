package nebflow.ir

import cats.effect.{IO, Ref}
import io.circe.syntax.*
import io.circe.Json

/**
 * 审计记录与落点端口（标准 §8.6）。
 *
 * 规则（都是 MUST）：
 *  - **每个节点必须落一条**，含 `Deny`/`Ask`（安全事件是重点，不是只记成功）；
 *  - **禁止**记录凭证值：含 `Secret` cap 的命令其 `argsDigest` **必须**是 HMAC（§8.2），
 *    `args` 原文**禁止**入账（可脱敏摘要）；
 *  - 审计**必须** best-effort 非阻塞（现行惯例 `core/ToolsLogWriter.scala`：内存队列满则
 *    丢弃并 WARN）；审计失败**禁止**影响执行结果。
 *
 * 落点（[T6] 待裁定）：本包只定义端口；`ToolsLogWriter` 在 core（本包层位看不见），
 * 故 P0 的落点在装配面 —— 换落点只改适配器一处，端口不变。
 */
final case class AuditRecord(
  ts: String,
  requestId: String,
  sessionId: String,
  tenant: String,
  ingress: String,
  ir: Int,
  safety: String,
  node: String,
  command: String,
  argsDigest: String,
  caps: List[String],
  capsSource: String,
  decision: String,
  rule: Option[String] = None,
  exit: Option[Int] = None,
  errorCode: Option[String] = None,
  durationMs: Option[Long] = None
):

  def toJson: Json =
    Json
      .obj(
        "ts" -> ts.asJson,
        "requestId" -> requestId.asJson,
        "sessionId" -> sessionId.asJson,
        "tenant" -> tenant.asJson,
        "ingress" -> ingress.asJson,
        "ir" -> ir.asJson,
        "safety" -> safety.asJson,
        "node" -> node.asJson,
        "command" -> command.asJson,
        "argsDigest" -> argsDigest.asJson,
        "caps" -> caps.asJson,
        "capsSource" -> capsSource.asJson,
        "decision" -> decision.asJson
      )
      .deepMerge(
        Json.obj(
          "rule" -> rule.map(_.asJson).getOrElse(Json.Null),
          "exit" -> exit.map(_.asJson).getOrElse(Json.Null),
          "errorCode" -> errorCode.map(_.asJson).getOrElse(Json.Null),
          "durationMs" -> durationMs.map(_.asJson).getOrElse(Json.Null)
        )
      )

end AuditRecord

/** 审计端口（best-effort：实现**禁止**抛，失败只 WARN）。 */
trait AuditSink:
  def record(rec: AuditRecord): IO[Unit]

object AuditSink:

  val noop: AuditSink = (_: AuditRecord) => IO.unit

  /** 测试用：内存累积（顺序 = 落账顺序）。 */
  def inMemory(ref: Ref[IO, Vector[AuditRecord]]): AuditSink =
    (rec: AuditRecord) => ref.update(_ :+ rec)

  def inMemory(): IO[(AuditSink, Ref[IO, Vector[AuditRecord]])] =
    Ref.of[IO, Vector[AuditRecord]](Vector.empty).map(ref => (inMemory(ref), ref))

end AuditSink
