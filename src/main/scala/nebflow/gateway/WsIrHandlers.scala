/* 命令 IR 路由层(Command IR Router P0)的装配面:WS ingress + 审计落点 + VFS 根/安全档注入。
 *
 * 本文件是 `nebflow.ir` 唯一「看得见上层」的地方(层序 `shared < actor < ir < core < …`):
 *   · VFS 根解析 → 复用既有单点 `resolveExplorerBaseRoot`([D20]:禁各处自行拼接);
 *   · 安全档 → `SharedResources.effectiveSafetyMode` 三值翻成 `ir.Safety`([D24]);
 *   · 审计落点 → 本文件(见 IrAuditSink;[T6] 待裁定:换落点只改这一处)。
 */
package nebflow.gateway

import cats.effect.IO
import io.circe.syntax.*
import io.circe.Json
import nebflow.ir.*
import nebflow.shared.{NebflowLogger, PathUtil}

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

/** 进程级装配：注册表/策略/审计口各一份（热注册是 P1 面，P0 只建一次）。 */
private[gateway] object IrGateway:

  private val logger = NebflowLogger.forName("nebflow.ir.gateway")

  private lazy val registry: CommandRegistry =
    val r = new CommandRegistry()
    DevCommands.base.foreach { cmd =>
      r.register(cmd) match
        case Left(err) =>
          throw new IllegalStateException(s"builtin command ${cmd.name} failed to register: ${err.message}")
        case Right(_) => ()
    }
    r

  /**
   * 策略：P0 用缺省表（无规则 ⇒ 纯读放行、其余 `Ask`；未知 cap fail-closed；`RealBash`
   * 危险判定 fail-closed 为「一律危险」）。规则表怎么存怎么配是 §8.2 划给实现的自由面 ——
   * 数据化配置在 P1 落地，此处保持**缺省即安全**。
   */
  private lazy val policy: PolicyEngine = new PolicyEngine(PolicyConfig())

  private lazy val auditSink: AuditSink = jsonlAuditSink()

  private lazy val router: Router = new Router(registry, policy, auditSink)

  def instance: Router = router

  /**
   * 审计落点：`<dataRoot>/logs/ir/<yyyyMMdd>.jsonl`（[T6] 的独立面选项；`ToolsLogWriter`
   * 在 core，本包层位看不见它，故落点由装配面提供）。best-effort：任何失败只 WARN，
   * **禁止**影响执行结果（§8.6）。单文件追加 + 每节点一次 `IO.blocking`；内存队列 +
   * 丢弃策略（`ToolsLogWriter` 形态）与轮转是 P1 的升级面。
   */
  def jsonlAuditSink(): AuditSink =
    val day = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)
    (rec: AuditRecord) =>
      IO.blocking {
        val path = PathUtil.dataRoot / "logs" / "ir" / s"${day.format(Instant.parse(rec.ts))}.jsonl"
        os.write.append(path, rec.toJson.noSpaces + "\n", createFolders = true)
      }.handleErrorWith(e => logger.warn(s"ir audit append failed: ${e.getMessage}"))

end IrGateway

/** 系统域扩展：`ir` 帧 —— 命令 IR 路由层的人侧入口（P0 直帧，P1 接人侧 argv 糖）。 */
private[gateway] object WsIrHandlers:

  private val logger = NebflowLogger.forName("nebflow.ir.ws")

  private[gateway] val handlers: Map[String, WsDispatch.WsHandler] = Map(
    "ir" -> handleIr
  )

  /**
   * 帧形态：`{"type":"ir","sessionId":"…","request":{<信封 §0>}}`。
   *
   * 传输层便利（不放松 IR 层严格性）：信封缺 `sessionId`/`requestId` 时用帧的 sessionId
   * 与一个新 UUID 补齐；信封自身的未知键/坏值仍由 `IrRequest.decode` 严格拒绝。
   */
  private def handleIr(
    ctx: WsDispatchCtx,
    text: String,
    wsSend: io.circe.Json => IO[Unit],
    watchSession: ExplorerWatchSession
  ): IO[Unit] =
    import ctx.*
    val frame = WsDispatch.parsedJson(text)
    val frameSessionId = frame.hcursor.downField("sessionId").as[String].getOrElse("")
    val envelope = frame.hcursor.downField("request").focus.getOrElse(Json.Null)
    val requestId = envelope.hcursor.downField("requestId").as[String].toOption.filter(_.nonEmpty)
    val sessionId =
      envelope.hcursor.downField("sessionId").as[String].toOption.filter(_.nonEmpty).getOrElse(frameSessionId)
    val prepared = envelope
      .deepMerge(Json.obj("sessionId" -> sessionId.asJson))
      .deepMerge(Json.obj("requestId" -> requestId.getOrElse(java.util.UUID.randomUUID().toString).asJson))

    if sessionId.isEmpty then
      wsSend(
        Json.obj("type" -> "irResult".asJson, "status" -> "invalid".asJson, "error" -> "sessionId is required".asJson)
      )
    else
      for
        rootStr <- resolveExplorerBaseRoot(sessionId, None)
        safety <- sharedResources.effectiveSafetyMode
        result <- IrGateway.instance.submit(prepared, VfsRoot(os.Path(rootStr, os.pwd)), irSafety(safety))
        _ <- wsSend(
          Json.obj(
            "type" -> "irResult".asJson,
            "requestId" -> prepared.hcursor.downField("requestId").as[String].getOrElse("").asJson,
            "response" -> result
          )
        )
      yield ()
    end if
  end handleIr

  /** `core.SafetyMode` → `ir.Safety`（唯一映射单点；两套枚举同 wire 值域，[D24]）。 */
  private def irSafety(mode: nebflow.core.SafetyMode): Safety = mode match
    case nebflow.core.SafetyMode.ConfirmEdits => Safety.ConfirmEdits
    case nebflow.core.SafetyMode.AutoEdits => Safety.AutoEdits
    case nebflow.core.SafetyMode.AutoAll => Safety.AutoAll

end WsIrHandlers
