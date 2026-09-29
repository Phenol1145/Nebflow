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
import nebflow.shared.{NebflowLogger, PathUtil, UiMessage}

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
   * 策略：P0 缺省表（无规则 ⇒ 纯读放行、其余 `Ask`；未知 cap fail-closed；`RealBash`
   * 危险判定 fail-closed 为「一律危险」）+ P1-2 批 A 的 `dev:tool:*` 一律 `Ask`
   * （[[IrToolCaps.askRule]]：桥接件全为 `prefix="*"` 的 VFS 外不透明宿主访问，
   * §8.1「无法做包含性判定 ⇒ 不假装能判」）。规则按名前缀只罩 `dev:tool:*` ⇒
   * `dev:fs:*` 的缺省行为不变；`auto-all` 档把 `Ask` 折 `Allow` 是既有档位语义。
   * 批 C（P1-2）接通危险 bash 组合底座：`dangerousBash` = [[IrToolBridge.bashDanger]]
   * （单源 `BashTool.isDangerous`）、`dangerousBashNames` = 桥接 bash 的 IR 名——
   * 仅当用户写显式 `Allow(dev:tool:bash)` 时危险命令被硬底拉回 `Ask`（rule
   * `bash:danger`）、安全命令放行；默认 askRule 下 `Ask+Ask=Ask` 逐字节不变。
   */
  private lazy val policy: PolicyEngine = new PolicyEngine(
    PolicyConfig(
      rules = IrToolCaps.askRules,
      dangerousBash = IrToolBridge.bashDanger,
      dangerousBashNames = Set(IrToolCaps.bashIrName)
    )
  )

  private lazy val auditSink: AuditSink = jsonlAuditSink()

  private lazy val router: Router = new Router(registry, policy, auditSink)

  def instance: Router = router

  /**
   * P1-3（LLM ingress 改道）llm 腿专用 Router：同 registry/policy/audit（审计单点不
   * 分叉），唯限额取 [[nebflow.gateway.IrLlmRoute.llmLimits]]（让位裁定：nodeTimeout/
   * maxTextBytes 放宽，使 IR 限额永不抢跑 declaredToolTimeoutMs/BashResilience/
   * ToolResultGuard——大 Read 不因 IR 抢跑从「持久化+预览」劣化为硬错误）。人侧糖腿/
   * 直连 ir 帧继续走 [[instance]]（默认限额，行为零变化）。
   */
  private[gateway] lazy val llmInstance: Router =
    new Router(registry, policy, auditSink, limits = IrLlmRoute.llmLimits)

  /**
   * P1-2 桥接装载入口（幂等 reindex）：把批 A/B/C 三十件内置 Tool 注册进 IR 命令表
   * （[[IrToolBridge.reindex]]：先摘后挂，单件失败记 ERROR 跳过）。`lib`（批 C） =
   * AgentLibrary 视图，注入 ToolContext 模板的 `agentLibrary` 槽（Delegate 真实
   * 可用；None ⇒ 确定性拒答）。时机钉：由
   * `GatewayMain.startMcpServers` 在 `loadExternalTools()`（ToolLoader.reload +
   * MCP startAll）之后调用——同 fiber 串行且晚于二者。同步面零抛（整体异常在此
   * 兜底记 ERROR，不炸 boot）。
   */
  def initBridge(sr: nebflow.core.AgentRuntimePort, lib: Option[nebflow.core.AgentLibraryView] = None): Unit =
    try IrToolBridge.reindex(registry, Some(sr), lib)
    catch
      case e: Throwable =>
        logger.error(s"IR tool bridge init failed: ${Option(e.getMessage).getOrElse(e.toString)}")

  /** 糖腿查表（P1-1）：`ArgvSugar.lower` 的 lookup 参数（params 是 argv 唯一权威面，§7.1）。 */
  def lookup(name: String): Option[CommandDef] = registry.get(name)

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

/**
 * `core.SafetyMode` → `ir.Safety` 的 gateway 单点（[D24] 唯一映射；两套枚举同 wire 值域）。
 * P1-3 从 WsIrHandlers 的 private def 上移：人侧糖腿与 llm 改道腿（IrLlmRoute）共用
 * 一份映射，消灭复制。
 */
private[gateway] object IrSafety:

  def of(mode: nebflow.core.SafetyMode): Safety = mode match
    case nebflow.core.SafetyMode.ConfirmEdits => Safety.ConfirmEdits
    case nebflow.core.SafetyMode.AutoEdits => Safety.AutoEdits
    case nebflow.core.SafetyMode.AutoAll => Safety.AutoAll

end IrSafety

/**
 * 会话 explorer 根解析的 gateway 单点（[D20]：禁各处自行拼接）。
 * P1-3 从 WebSocketRoutes 的 private def 上移（函数体逐字迁移）：explorer-rt 腿
 * （WebSocketRoutes）、人侧糖腿/直连 ir 帧（WsIrHandlers 经 WsDispatchCtx 注入的
 * impl 最终同源）与 llm 改道腿（IrLlmRoute）共用一份。
 */
private[gateway] object ExplorerRoots:

  def resolve(
    sessionStore: nebflow.core.SessionStore,
    sessionId: String,
    overrideRoot: Option[String]
  ): IO[String] =
    overrideRoot match
      case Some(root) => IO.pure(root)
      case None =>
        for
          metaOpt <- sessionStore.getSessionMeta(sessionId)
          folderId = metaOpt.flatMap(_.folderId)
          prOpt <- sessionStore.resolveProjectRoot(folderId)
        yield prOpt.getOrElse((PathUtil.dataRoot / "projects").toString)

end ExplorerRoots

/** 系统域扩展：`ir` 帧 —— 命令 IR 路由层的人侧入口（P0 直帧，P1 接人侧 argv 糖）。 */
private[gateway] object WsIrHandlers:

  private val logger = NebflowLogger.forName("nebflow.ir.ws")

  private[gateway] val handlers: Map[String, WsDispatch.WsHandler] = Map(
    "ir" -> handleIr
  )

  /** 人侧三腿：typeless（无 `type` 键）+ `immediateInput`（浏览器）+ `userMessage`（CLI）。 */
  private val SugarLegs: Set[String] = Set("", "immediateInput", "userMessage")

  /** 附件/引用类键：任一非空 ⇒ 不 lower（fail-open——糖是纯文本特性，转发/附件语义不归 IR）。 */
  private val SugarPayloadKeys: List[String] = List("attachments", "refs", "taskRefs")

  /**
   * 分流闸（P1-1，纯函数可单测；`WebSocketRoutes.handleMessage` 在 msgType 取值之后、
   * `WsDispatch.handlers` 派发之前调用——一处覆盖三腿 + REST `handleMessagePublic`）：
   *
   * 命中 = msgType ∈ 三腿 ∧ content（trim）非空 ∧ attachments/refs/taskRefs 键全空 ∧
   * `ArgvSugar.humanShape(content)`（首段 ∈ {dev,mcp,ext,bash} 且 ≥2 段；`//` 转义与
   * `/clear` 族保留名天然被拒——白名单正向判定的补就是保留名集合，无需另维护清单）。
   * §9:547 的「未被既有闸消费的 `/…` 才 lower」由此钉死：命中 ⇒ 整帧归 IR 糖腿
   * （不进 WsDispatch 既有 handler、不投 `AgentCommand`）；未命中 ⇒ 既有腿逐字节不变。
   */
  def shouldLowerToSugar(msgType: String, frame: Json): Boolean =
    SugarLegs.contains(msgType) && {
      val content = frame.hcursor.downField("content").as[String].getOrElse("")
      content.trim.nonEmpty &&
      SugarPayloadKeys.forall(k => payloadEmpty(frame, k)) &&
      ArgvSugar.humanShape(content)
    }

  /** 键缺席/null/空数组 ⇒ 空；非空数组或任何非数组非 null 值 ⇒ 非空（fail-open 不 lower）。 */
  private def payloadEmpty(frame: Json, key: String): Boolean =
    frame.hcursor.downField(key).focus match
      case None => true
      case Some(v) => v.isNull || v.asArray.exists(_.isEmpty)

  /**
   * 人侧 argv 糖的 lowering 入口（P1-1）。准入不得低于文本腿（热重启批 §3.3 的
   * choke 点同款）：先 `admitWorkOrRefuse`（draining 拒绝并回 workRefused），体内
   * `IO.uncancelable`（断连存活，同 typeless 腿先例）。顺序：
   *
   *   sessionId 空回 invalid 帧（同 handleIr 形）→ user 气泡（UiMessage.User 原文，
   *   记录面同 dispatchUserText）→ `ArgvSugar.lower` → Left ⇒ `Router.invalidResult`
   *   同形错误；Right ⇒ 服务端构信封（tenant=human/console 常量、ingress=human、
   *   requestId=UUID）submit。两条出口都回 `irResult` 帧 + `ir.sugar` 系统气泡
   *   （best-effort warn 不阻塞）。
   */
  def handleHumanSugar(ctx: WsDispatchCtx, text: String, wsSend: Json => IO[Unit]): IO[Unit] =
    import ctx.*
    val frame = WsDispatch.parsedJson(text)
    val sessionId = frame.hcursor.downField("sessionId").as[String].getOrElse("")
    val content = frame.hcursor.downField("content").as[String].getOrElse("")
    if sessionId.isEmpty then
      wsSend(
        Json.obj("type" -> "irResult".asJson, "status" -> "invalid".asJson, "error" -> "sessionId is required".asJson)
      )
    else
      ctx.admitWorkOrRefuse(wsSend)(
        IO.uncancelable(_ =>
          sessionStore
            .appendUiMessages(sessionId, List(UiMessage.User(content, Nil, timestamp = System.currentTimeMillis())))
            .handleErrorWith(e => ctx.logger.warn(s"ir sugar: failed to record user bubble: ${e.getMessage}")) *>
            runSugar(ctx, sessionId, content, wsSend)
        )
      )
  end handleHumanSugar

  private def runSugar(ctx: WsDispatchCtx, sessionId: String, content: String, wsSend: Json => IO[Unit]): IO[Unit] =
    import ctx.*
    val requestId = java.util.UUID.randomUUID().toString
    def bubble(result: PlanResult): IO[Unit] =
      sessionStore
        .appendUiMessages(sessionId, List(sugarSystem(content, result)))
        .handleErrorWith(e => ctx.logger.warn(s"ir sugar: failed to record system bubble: ${e.getMessage}"))
    def sendResult(result: PlanResult): IO[Unit] =
      wsSend(
        Json.obj(
          "type" -> "irResult".asJson,
          "requestId" -> requestId.asJson,
          "response" -> result.toJson
        )
      ) *> bubble(result)
    ArgvSugar.lower(content, IrGateway.lookup) match
      case Left(err) =>
        sendResult(IrGateway.instance.invalidResult(requestId, err))
      case Right(call) =>
        for
          rootStr <- resolveExplorerBaseRoot(sessionId, None)
          safety <- sharedResources.effectiveSafetyMode
          result <- IrGateway.instance.submit(
            // 服务端构信封：tenant=human/console 常量（gateway 只有 token 级 auth，无逐用户
            // 身份——openQuestions 待作者裁定）、ingress=human、requestId=UUID
            IrRequest(call, Tenant.Human("console"), Ingress.Human, sessionId, requestId),
            VfsRoot(os.Path(rootStr, os.pwd)),
            IrSafety.of(safety)
          )
          _ <- sendResult(result)
        yield ()
    end match
  end runSugar

  /** `ir.sugar` 系统气泡：完整可读摘要（命令名/status/exit/error.code；先例 slash.clearDone）。 */
  private def sugarSystem(content: String, result: PlanResult): UiMessage.System =
    val cmd = content.trim.takeWhile(c => c != ' ' && c != '\t' && c != '\n' && c != '\r')
    val exit = result.exit.map(_.toString).getOrElse("null")
    val err = result.error.map(e => s", ${e.code}").getOrElse("")
    UiMessage.System(s"$cmd → ${Status.wire(result.status)} (exit=$exit$err)", i18nKey = Some("ir.sugar"))

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
        result <- IrGateway.instance.submit(prepared, VfsRoot(os.Path(rootStr, os.pwd)), IrSafety.of(safety))
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

end WsIrHandlers
