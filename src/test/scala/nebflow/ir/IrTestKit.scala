package nebflow.ir

import cats.effect.{IO, Ref}
import io.circe.Json
import io.circe.syntax.*

/**
 * P0 一致性用例的共用夹具：临时 VFS 根 + 内置命令 + 可选测试命令 + 可注入规则/限额/时钟。
 * 断言面永远是**规范条款**（用例编号 C1…C40 见 `.zcode/plans/command-ir-standard.md` §11）。
 */
object IrTestKit:

  final case class Harness(
    root: os.Path,
    router: Router,
    audit: Ref[IO, Vector[AuditRecord]],
    counter: Ref[IO, Int],
    policy: PolicyEngine
  ):
    def records: IO[Vector[AuditRecord]] = audit.get

  /** 计数型副作用命令：cap 非纯读 ⇒ 无规则时缺省 `Ask`（三态用例的载体）。 */
  def probe(
    counter: Ref[IO, Int],
    caps: Set[Cap] = Set(Cap.FsWrite(".")),
    name: String = "dev:test:probe",
    stdin: Option[StreamKind] = None
  ): CommandDef =
    CommandDef(
      name = name,
      description = "counting probe (test only)",
      argsSchema = Json.obj("type" -> "object".asJson, "additionalProperties" -> false.asJson).asObject.get,
      binding = Binding.Dev((_, _) => counter.updateAndGet(_ + 1).map(n => Right(StreamValue.Text(s"probe#$n")))),
      io = CommandIo(stdin, StreamKind.Text),
      caps = caps,
      audiences = Set(Audience.Human)
    )

  /** 只从 stdin 读的 JSONL 消费者（管道类型匹配与 `Redirect(in)` 的载体）。 */
  def sink(name: String = "dev:test:sink"): CommandDef =
    CommandDef(
      name = name,
      description = "jsonl sink (test only)",
      argsSchema = Json.obj("type" -> "object".asJson).asObject.get,
      binding = Binding.Dev((_, ctx) => IO.pure(Right(StreamValue.Text(s"got ${ctx.stdin.map(_.size).getOrElse(0)}")))),
      io = CommandIo(Some(StreamKind.Jsonl), StreamKind.Text),
      caps = Set.empty,
      audiences = Set(Audience.Human)
    )

  /** 固定文本产出（限额 / J6 声明与实况不符的载体）。 */
  def emitter(text: String, declared: StreamKind = StreamKind.Text, name: String = "dev:test:emit"): CommandDef =
    CommandDef(
      name = name,
      description = "fixed emitter (test only)",
      argsSchema = Json.obj("type" -> "object".asJson).asObject.get,
      binding = Binding.Dev((_, _) => IO.pure(Right(StreamValue.Text(text)))),
      io = CommandIo(None, declared),
      caps = Set.empty,
      audiences = Set(Audience.Human)
    )

  /** 立即失败的命令（fail-fast / Redirect 先判后写的载体）。 */
  val boom: CommandDef =
    CommandDef(
      name = "dev:test:boom",
      description = "always fails (test only)",
      argsSchema = Json.obj("type" -> "object".asJson).asObject.get,
      binding = Binding.Dev((_, _) => IO.pure(Left(IrError.commandFailed("boom")))),
      io = CommandIo(None, StreamKind.Text),
      caps = Set.empty,
      audiences = Set(Audience.Human)
    )

  def harness(
    root: os.Path,
    rules: List[PolicyRule] = Nil,
    extra: List[CommandDef] = Nil,
    limits: IrLimits = IrLimits.default,
    knownCapKinds: Set[String] = PolicyConfig().knownCapKinds,
    registry: Option[CommandRegistry] = None
  ): IO[Harness] =
    for
      counter <- Ref.of[IO, Int](0)
      auditRef <- Ref.of[IO, Vector[AuditRecord]](Vector.empty)
      reg <- IO.pure(registry.getOrElse {
        val r = new CommandRegistry()
        // probe 声明 stdin 能力：管道用例里它可以是二级消费者（[D19]：声明是能力不是义务）
        val base = DevCommands.base ++ List(probe(counter, stdin = Some(StreamKind.Text)), sink(), boom) ++ extra
        base.foreach(c =>
          r.register(c) match
            case Left(e) => throw new IllegalStateException(s"test command registration failed: ${e.message}")
            case Right(_) => ()
        )
        r
      })
    yield
      val policy = new PolicyEngine(PolicyConfig(knownCapKinds = knownCapKinds, rules = rules))
      val router = new Router(reg, policy, AuditSink.inMemory(auditRef), limits)
      Harness(root, router, auditRef, counter, policy)

  def vfs(files: (String, String)*): IO[os.Path] =
    IO.blocking {
      val dir = os.temp.dir(prefix = "nebflow-ir-")
      files.foreach { case (rel, content) =>
        val p = rel.split("/").foldLeft(dir)((acc, seg) => acc / seg)
        os.write(p, content, createFolders = true)
      }
      dir
    }

  def req(
    plan: Ir,
    tenant: Tenant = Tenant.Human("local"),
    ingress: Ingress = Ingress.Human,
    approval: Option[ApprovalCredential] = None,
    requestId: String = "req-1"
  ): IrRequest = IrRequest(plan, tenant, ingress, "sess-1", requestId, approval)

  def envelopeJson(
    plan: Ir,
    tenant: Tenant = Tenant.Human("local"),
    ingress: Ingress = Ingress.Human,
    approval: Option[ApprovalCredential] = None
  ): Json =
    Json
      .obj(
        "ir" -> Ir.Version.asJson,
        "plan" -> Ir.encode(plan),
        "tenant" -> Tenant.toJson(tenant),
        "ingress" -> Ingress.wire(ingress).asJson,
        "sessionId" -> "sess-1".asJson,
        "requestId" -> "req-1".asJson
      )
      .deepMerge(approval.map(a => Json.obj("approval" -> credJson(a))).getOrElse(Json.obj()))

  def credJson(c: ApprovalCredential): Json =
    Json.obj(
      "planDigest" -> c.planDigest.asJson,
      "capsDigest" -> c.capsDigest.asJson,
      "approvedBy" -> c.approvedBy.asJson,
      "approvedAt" -> c.approvedAt.asJson,
      "expiresAt" -> c.expiresAt.asJson,
      "nonce" -> c.nonce.asJson
    )

  /** 从 `await_approval` 响应里取审批体并铸一张有效凭据（模拟人侧点击"批准"）。 */
  def approve(result: PlanResult, tenant: Tenant = Tenant.Human("local"), nonce: String = "n-1"): ApprovalCredential =
    val ap = result.approval.getOrElse(throw new IllegalStateException("no approval request in result"))
    ApprovalCredential(
      planDigest = ap.planDigest,
      capsDigest = ap.capsDigest,
      approvedBy = Tenant.render(tenant),
      approvedAt = "2026-09-28T12:00:00Z",
      expiresAt = "2099-01-01T00:00:00Z",
      nonce = nonce
    )

  def call(name: String, path: Option[String] = None): Ir =
    Ir.Call(name, path.fold(io.circe.JsonObject.empty)(p => io.circe.JsonObject("path" -> p.asJson)))

  def codes(records: Vector[AuditRecord]): List[String] = records.map(r => s"${r.node}:${r.decision}").toList

end IrTestKit
