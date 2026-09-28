package nebflow.ir

import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Json, JsonObject}
import nebflow.shared.NebflowLogger

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * 请求信封（标准 §0）。**信封不是节点**：§3.2/R1 的「恰好含四个标签键之一」只作用于
 * `plan` 的**值**；信封有自己的键集与规则（含未知键 ⇒ `router.schema.unknown_key`）。
 *
 * 计划体是**纯的**（[I2]：无身份/时间/随机/凭证）——租户、ingress、requestId、审批
 * 凭据一律在信封里，计划因此可缓存、可比较、可离线审批（[D8] 的一半收益在这里）。
 */
final case class IrRequest(
  plan: Ir,
  tenant: Tenant,
  ingress: Ingress,
  sessionId: String,
  requestId: String,
  approval: Option[ApprovalCredential] = None,
  ir: Int = Ir.Version
)

object IrRequest:

  private val Keys = Set("ir", "plan", "tenant", "ingress", "sessionId", "requestId", "approval")

  def decode(json: Json): Either[IrError, IrRequest] =
    json.asObject match
      case None => Left(IrError.nodeTag("the request envelope must be a JSON object"))
      case Some(o) =>
        val unknown = o.keys.filterNot(Keys.contains).toList.sorted
        if unknown.nonEmpty then Left(IrError.unknownKey(unknown.head))
        else
          for
            version <- o("ir") match
              case Some(v) =>
                v.asNumber.flatMap(_.toInt).toRight(IrError.badValue("envelope 'ir' must be an integer"))
              case None => Left(IrError.invalidArgs("envelope 'ir' is required", List(".ir")))
            _ <-
              if version == Ir.Version then Right(())
              else Left(IrError.badVersion(version, Ir.Version))
            _ <- Right(()) // 版本先于其它校验（§10.1：版本不匹配优先于任何执行动作）
            planJson <- o("plan").toRight(IrError.invalidArgs("envelope 'plan' is required", List(".plan")))
            plan <- Ir.decode(planJson)
            tenant <- o("tenant")
              .toRight(IrError.invalidArgs("envelope 'tenant' is required", List(".tenant")))
              .flatMap(Tenant.decode)
            ingress <- o("ingress").flatMap(_.asString) match
              case None => Left(IrError.invalidArgs("envelope 'ingress' is required", List(".ingress")))
              case Some(raw) =>
                Ingress.parse(raw).toRight(IrError.badValue(s"unknown ingress '$raw' (human|llm|script|external)"))
            _ <-
              // 信封↔租户一致性（§0，C40）：脚本由谁触发不改变权限主体
              if Tenant.allows(tenant, ingress) then Right(())
              else
                Left(
                  IrError
                    .badValue(
                      s"tenant kind '${Tenant.kind(tenant)}' is inconsistent with ingress '${Ingress.wire(ingress)}'"
                    )
                    .withDetail("tenant", Tenant.render(tenant).asJson)
                    .withDetail("ingress", Ingress.wire(ingress).asJson)
                )
            sessionId <- required(o, "sessionId")
            requestId <- required(o, "requestId")
            approval <- o("approval").filterNot(_.isNull) match
              case None => Right(None)
              case Some(a) => ApprovalCredential.decode(a).map(Some(_))
          yield IrRequest(plan, tenant, ingress, sessionId, requestId, approval, version)

        end if

  private def required(o: JsonObject, key: String): Either[IrError, String] =
    o(key).flatMap(_.asString).filter(_.nonEmpty) match
      case Some(v) => Right(v)
      case None => Left(IrError.invalidArgs(s"envelope '$key' is required", List(s".$key")))

end IrRequest

/**
 * 审批凭据（§8.5）。**5a 是闭环的唯一入口**：审批凭据是唯一能把 `Ask` 翻成 `Allow` 的输入——
 * 缺了它，重提交必然再次 `Ask`（死循环，[D26]）。绑定 `(planDigest, capsDigest, tenant)`
 * 三者，单次使用（`nonce` 消费后失效），TTL 建议 10 分钟（[T5]）。
 */
final case class ApprovalCredential(
  planDigest: String,
  capsDigest: String,
  approvedBy: String,
  approvedAt: String,
  expiresAt: String,
  nonce: String
)

object ApprovalCredential:

  private val Keys = Set("planDigest", "capsDigest", "approvedBy", "approvedAt", "expiresAt", "nonce")

  def decode(json: Json): Either[IrError, ApprovalCredential] =
    json.asObject match
      case None => Left(IrError.badValue("approval must be a JSON object"))
      case Some(o) =>
        val unknown = o.keys.filterNot(Keys.contains).toList.sorted
        if unknown.nonEmpty then Left(IrError.unknownKey(unknown.head))
        else
          (
            field(o, "planDigest"),
            field(o, "capsDigest"),
            field(o, "approvedBy"),
            field(o, "approvedAt"),
            field(o, "expiresAt"),
            field(o, "nonce")
          ).mapN(ApprovalCredential.apply)

  private def field(o: JsonObject, key: String): Either[IrError, String] =
    o(key).flatMap(_.asString).filter(_.nonEmpty) match
      case Some(v) => Right(v)
      case None => Left(IrError.badValue(s"approval.$key is required"))

end ApprovalCredential

/** 一次性 `nonce` 存储（§8.5 单次使用）。实现必须线程安全：`.add` 的返回值即首次使用判据。 */
trait NonceStore:
  def consume(nonce: String): IO[Boolean]

object NonceStore:

  def inMemory: NonceStore =
    val seen = ConcurrentHashMap.newKeySet[String]()
    (nonce: String) => IO.blocking(seen.add(nonce))

end NonceStore

/** 审批凭据的失败分道（§8.5）：`Misuse` ⇒ rejected；`Stale` ⇒ 重新 `await_approval`。 */
enum ApprovalFailure:
  case Misuse(err: IrError)
  case Stale(err: IrError)

/**
 * 路由层门面：信封进、信封出（标准 §0/§6.5/§8.4）。
 *
 * ```
 * lowering → 校验 → 整计划逐节点 decide → Deny ⇒ rejected（零执行）
 *                                     → Ask  ⇒ 凭据有效则折算 Allow，否则 await_approval（零执行）
 *                                     → 全 Allow ⇒ 执行
 * ```
 *
 * 装配面（gateway）负责三件本层看不见的事：**VFS 根解析**（[D20] 单点）、**安全档**
 * 映射（[D24]，`SharedResources.effectiveSafetyMode` → [[Safety]]）、**审计落点**（[T6]）。
 */
final class Router(
  val registry: CommandRegistry,
  val policy: PolicyEngine,
  val audit: AuditSink,
  val limits: IrLimits = IrLimits.default,
  nonces: NonceStore = NonceStore.inMemory,
  hmacKey: Array[Byte] = Router.randomKey(),
  now: IO[Instant] = IO.realTime.map(d => Instant.ofEpochMilli(d.toMillis))
):

  private val logger = NebflowLogger.forName("nebflow.ir.router")

  /** JSON 形态的入口（WS/REST 帧用）：信封 → 响应信封。 */
  def submit(json: Json, root: VfsRoot, safety: Safety): IO[Json] =
    IrRequest.decode(json) match
      case Left(err) =>
        // 信封坏掉时 requestId 仍尽力取回（审计/回执要能对上那一次调用）
        val rid = json.hcursor.downField("requestId").as[String].toOption.getOrElse("")
        IO.pure(invalid(rid, err).toJson)
      case Right(req) => submit(req, root, safety).map(_.toJson)

  def submit(req: IrRequest, root: VfsRoot, safety: Safety): IO[PlanResult] =
    if req.ir != Ir.Version then IO.pure(invalid(req.requestId, IrError.badVersion(req.ir, Ir.Version)))
    else if !Tenant.allows(req.tenant, req.ingress) then
      IO.pure(
        invalid(
          req.requestId,
          IrError.badValue(
            s"tenant kind '${Tenant.kind(req.tenant)}' is inconsistent with ingress '${Ingress.wire(req.ingress)}'"
          )
        )
      )
    else
      val ctx = CallCtx(req.tenant, req.ingress, req.sessionId, req.requestId, Ir.RootPath, root, safety, limits)
      // 计划期含一次 IO（路径 realpath，[P8]），故整条链是 IO
      Planner.plan(req.plan, ctx, registry, policy, limits, hmacKey).flatMap {
        case Left(err) =>
          // 计划非法：没有任何节点 ⇒ 无节点级审计（§8.6 的「每节点一条」在此为空集）
          logger.warn(s"ir plan rejected as invalid: ${err.code}").as(invalid(req.requestId, err))
        case Right(plan) => settle(req, plan, ctx)
      }

  // ── §8.4 步 4/5/6 ─────────────────────────────────────────

  private def settle(req: IrRequest, plan: Plan, ctx: CallCtx): IO[PlanResult] =
    plan.denied match
      case Some(node) =>
        val (reason, rule) = node.decision match
          case Decision.Deny(r, ru) => (r, ru)
          case _ => ("denied", "policy")
        audit(plan, ctx, Map.empty) *>
          IO.pure(
            PlanResult(
              requestId = req.requestId,
              status = Status.Rejected,
              exit = Some(ExitCode.Rejected),
              error = Some(IrError.policyDenied(reason, rule, node.node))
            )
          )
      case None =>
        plan.firstAsk match
          case None => execute(req, plan, ctx)
          case Some(askNode) =>
            val (reason, rule) = askNode.decision match
              case Decision.Ask(r, ru) => (r, ru)
              case _ => ("approval required", "policy")
            req.approval match
              // 5a：凭据有效 ⇒ 该 Ask 折算为 Allow，继续执行（[D26] 的闭环唯一入口）
              case Some(cred) =>
                checkApproval(cred, plan, ctx).flatMap {
                  case Right(()) => execute(req, plan, ctx)
                  case Left(ApprovalFailure.Misuse(err)) =>
                    audit(plan, ctx, Map.empty).as(
                      PlanResult(req.requestId, Status.Rejected, Some(ExitCode.Rejected), error = Some(err))
                    )
                  // capsDigest 漂移（TOCTOU，C38）：凭据失效 ⇒ 重新 await_approval（§8.5）
                  case Left(ApprovalFailure.Stale(err)) =>
                    audit(plan, ctx, Map.empty).as(
                      PlanResult(
                        req.requestId,
                        Status.AwaitApproval,
                        None,
                        error = Some(err),
                        approval = Some(approvalRequest(plan, reason, rule))
                      )
                    )
                }
              // 5b：无凭据 ⇒ await_approval（**零执行**，exit 为 null）
              case None =>
                audit(plan, ctx, Map.empty).as(
                  PlanResult(
                    req.requestId,
                    Status.AwaitApproval,
                    None,
                    approval = Some(approvalRequest(plan, reason, rule))
                  )
                )

            end match

  private def execute(req: IrRequest, plan: Plan, ctx: CallCtx): IO[PlanResult] =
    Executor.run(plan, ctx, limits, policy).flatMap { exec =>
      val byNode = exec.results.map(r => r.node -> r).toMap
      audit(plan, ctx, byNode).as {
        val failed = exec.results.find(_.exit != ExitCode.Ok)
        PlanResult(
          requestId = req.requestId,
          status = Status.Done,
          exit = Some(failed.map(_.exit).getOrElse(ExitCode.Ok)),
          results = exec.results.toList,
          finalStdout = exec.results.lastOption.flatMap(_.stdout),
          error = failed.flatMap(_.error).map { e =>
            exec.interruptedAt.fold(e)(n => e.withDetail("node", n.asJson))
          }
        )
      }
    }

  private def approvalRequest(plan: Plan, reason: String, rule: String): ApprovalRequest =
    ApprovalRequest(
      planDigest = plan.planDigest,
      capsDigest = plan.capsDigest,
      nodes = plan.nodes.toList.map(n => ApprovalNode(n.node, n.target.command, n.target.capNames, n.argsDigest)),
      reason = reason,
      rule = rule
    )

  /**
   * 凭据校验（§8.5）。两类失败分道：
   *  - [[ApprovalFailure.Misuse]]：`planDigest`／租户／过期／nonce 重放 ⇒ `policy.denied`
   *    （审批**禁止**被挪用/扩大，C11/C35）；
   *  - [[ApprovalFailure.Stale]]：`capsDigest` 不匹配 ⇒ 凭据失效，**重新** `await_approval`
   *    （registry 支持热注册 ⇒ 描述符可漂移，没有 capsDigest 就有 TOCTOU，[D25]/C38）。
   */
  private def checkApproval(cred: ApprovalCredential, plan: Plan, ctx: CallCtx): IO[Either[ApprovalFailure, Unit]] =
    def misuse(reason: String, rule: String): IO[Either[ApprovalFailure, Unit]] =
      IO.pure(Left(ApprovalFailure.Misuse(IrError.policyDenied(reason, rule, Ir.RootPath))))

    if cred.planDigest != plan.planDigest then
      misuse("approval was granted for a different plan", "approval:planDigest")
    else if cred.approvedBy != Tenant.render(ctx.tenant) then
      misuse("approval was granted to a different tenant", "approval:tenant")
    else if cred.capsDigest != plan.capsDigest then
      IO.pure(
        Left(
          ApprovalFailure.Stale(
            IrError(Codes.PolicyDenied, "command capabilities changed since the approval was granted (TOCTOU)")
              .withDetail("rule", "approval:capsDigest".asJson)
          )
        )
      )
    else
      parseInstant(cred.expiresAt) match
        case None => misuse("approval credential is malformed (expiresAt is not an ISO instant)", "approval:expiresAt")
        case Some(exp) =>
          for
            t <- now
            checked <-
              if !t.isBefore(exp) then misuse("approval credential has expired", "approval:expired")
              else
                nonces.consume(cred.nonce).flatMap { fresh =>
                  if fresh then IO.pure(Right(()))
                  else misuse("approval nonce was already used (single use)", "approval:nonce")
                }
          yield checked
    end if
  end checkApproval

  private def parseInstant(raw: String): Option[Instant] =
    try Some(Instant.parse(raw))
    catch case _: Exception => None

  // ── 审计（§8.6） ──────────────────────────────────────────

  /**
   * 每个**已判定节点**落一条（含 `Deny`/`Ask`：安全事件是重点，不是只记成功）。已执行
   * 的带 exit/duration；被短路（fail-fast）或整计划零执行（rejected/await_approval）的
   * 记录 `exit = null` —— 与 §6.4「被短路未执行的节点不出现在结果中」互补：结果面缺席，
   * 审计面在场。best-effort：审计失败**禁止**影响执行结果。
   */
  private def audit(plan: Plan, ctx: CallCtx, results: Map[String, NodeResult]): IO[Unit] =
    now
      .flatMap { t =>
        plan.nodes.toList
          .traverse_ { n =>
            val res = results.get(n.node)
            audit
              .record(
                AuditRecord(
                  ts = t.toString,
                  requestId = ctx.requestId,
                  sessionId = ctx.sessionId,
                  tenant = Tenant.render(ctx.tenant),
                  ingress = Ingress.wire(ctx.ingress),
                  ir = Ir.Version,
                  safety = Safety.wire(ctx.safety),
                  node = n.node,
                  command = n.target.command,
                  argsDigest = n.argsDigest,
                  caps = n.target.capNames,
                  capsSource = capsSource(n),
                  decision = Decision.wire(n.decision),
                  rule = Decision.ruleOf(n.decision),
                  exit = res.map(_.exit),
                  errorCode = res.flatMap(_.error).map(_.code),
                  durationMs = res.map(_.durationMs)
                )
              )
              .handleErrorWith(e => logger.warn(s"ir audit write failed for node ${n.node}: ${e.getMessage}"))
          }
      }
      .handleErrorWith(e => logger.warn(s"ir audit failed: ${e.getMessage}"))

  private def capsSource(n: PlannedNode): String =
    if n.target.command.startsWith("@redirect:") then Codes.CapsRedirect
    else if n.target.capKinds.forall(policy.config.knownCapKinds.contains) then Codes.CapsDeclared
    else Codes.CapsPolicyUnknown

  private def invalid(requestId: String, err: IrError): PlanResult =
    PlanResult(requestId = requestId, status = Status.Invalid, exit = Some(exitFor(err)), error = Some(err))

  /** §6.4：`status=invalid` 的计划级退出码 = 首个致因的码。 */
  private def exitFor(err: IrError): Int = err.code match
    case Codes.UnknownCommand => ExitCode.UnknownCommand
    case Codes.BindingUnavailable => ExitCode.NotExecutable
    case Codes.Timeout => ExitCode.Timeout
    case Codes.Cancelled => ExitCode.Cancelled
    case _ => ExitCode.Usage

end Router

object Router:

  def randomKey(): Array[Byte] =
    val k = new Array[Byte](32)
    new java.security.SecureRandom().nextBytes(k)
    k
end Router
