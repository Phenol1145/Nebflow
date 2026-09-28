package nebflow.ir

import io.circe.syntax.*
import io.circe.{Json, JsonObject}

/**
 * 调用上下文：租户、ingress、安全档（标准 §8.2/§8.3、§9）。
 *
 * **层位说明**：`nebflow.ir` 位于 `shared < actor < ir < core` 之下，故本包**看不见**
 * `core.permissions.SafetyMode`。此处定义的 [[Safety]] 是同一份 wire 契约（三档显式值域
 * `confirm-edits`/`auto-edits`/`auto-all`，见 `core/permissions.scala:48 wireValues`）的
 * IR 侧镜像；**唯一映射单点** = 装配面适配器（gateway 把 `SharedResources.effectiveSafetyMode`
 * 的三值翻成本枚举，单向注入）。档位语义与 caps 的合成规则见 [[PolicyEngine]]。
 */
enum Safety:
  case ConfirmEdits, AutoEdits, AutoAll

object Safety:

  def wire(s: Safety): String = s match
    case ConfirmEdits => "confirm-edits"
    case AutoEdits => "auto-edits"
    case AutoAll => "auto-all"

  /** 严格解析（同 `SafetyMode.fromWire`：写入口不用静默兜底）。 */
  def fromWire(s: String): Option[Safety] = s match
    case "confirm-edits" => Some(ConfirmEdits)
    case "auto-edits" => Some(AutoEdits)
    case "auto-all" => Some(AutoAll)
    case _ => None

end Safety

/** 租户种类（§8.3）。 */
enum TenantKind:
  case Human, Llm, External

/** 四个 ingress（§9 表）。 */
enum Ingress:
  case Human, Llm, Script, External

object Ingress:

  def wire(i: Ingress): String = i match
    case Human => "human"
    case Llm => "llm"
    case Script => "script"
    case External => "external"

  def parse(raw: String): Option[Ingress] = raw match
    case "human" => Some(Human)
    case "llm" => Some(Llm)
    case "script" => Some(Script)
    case "external" => Some(External)
    case _ => None

end Ingress

/** 三个可见面（§7.4）。 */
enum Audience:
  case Human, Llm, Script, McpClient

/**
 * `Ingress ↔ Audience` 是 **1:1 映射**（§7.4）：human↔Human、llm↔Llm、script↔Script、
 * external↔McpClient（两套枚举名字不同但语义一一对应，检查点按此映射执行）。
 */
object Audience:

  def of(ingress: Ingress): Audience = ingress match
    case Ingress.Human => Human
    case Ingress.Llm => Llm
    case Ingress.Script => Script
    case Ingress.External => McpClient
end Audience

/**
 * 租户（§8.3 / §0 信封 `tenant`）：`Human(user) | Llm(sessionId, agent) | External(serverId)`。
 * 判定输入 = IR 数据 + 声明 caps + 租户/ingress + 安全档（§8.2）——租户在**信封**里，
 * 不在计划体内（[I2]：计划是纯的，可缓存、可比较、可离线审批）。
 */
enum Tenant:
  case Human(user: String)
  case Llm(session: String, agent: String)
  case External(server: String)

object Tenant:

  def kind(t: Tenant): TenantKind = t match
    case _: Human => TenantKind.Human
    case _: Llm => TenantKind.Llm
    case _: External => TenantKind.External

  /** 审计与审批绑定用的稳定主体串（`human:local` / `llm:<sess>/<agent>` / `external:<server>`）。 */
  def render(t: Tenant): String = t match
    case Human(user) => s"human:$user"
    case Llm(session, agent) => s"llm:$session/$agent"
    case External(server) => s"external:$server"

  def toJson(t: Tenant): Json = t match
    case Human(user) => Json.obj("kind" -> "human".asJson, "user" -> user.asJson)
    case Llm(session, agent) =>
      Json.obj("kind" -> "llm".asJson, "session" -> session.asJson, "agent" -> agent.asJson)
    case External(server) => Json.obj("kind" -> "external".asJson, "server" -> server.asJson)

  /** 严格解码：未知键/未知 kind/缺字段一律拒绝（[I8]）。 */
  def decode(json: Json): Either[IrError, Tenant] =
    json.asObject match
      case None => Left(IrError.badValue("tenant must be a JSON object"))
      case Some(o) =>
        val kindRaw = o("kind").flatMap(_.asString)
        val allowed = kindRaw match
          case Some("human") => Set("kind", "user")
          case Some("llm") => Set("kind", "session", "agent")
          case Some("external") => Set("kind", "server")
          case _ => Set("kind")
        val unknown = o.keys.filterNot(allowed.contains).toList.sorted
        if unknown.nonEmpty then Left(IrError.unknownKey(unknown.head))
        else
          def field(k: String): Option[String] = o(k).flatMap(_.asString).filter(_.nonEmpty)
          kindRaw match
            case Some("human") =>
              field("user").toRight(IrError.badValue("tenant.user is required for kind=human")).map(Human.apply)
            case Some("llm") =>
              for
                s <- field("session").toRight(IrError.badValue("tenant.session is required for kind=llm"))
                a <- field("agent").toRight(IrError.badValue("tenant.agent is required for kind=llm"))
              yield Llm(s, a)
            case Some("external") =>
              field("server")
                .toRight(IrError.badValue("tenant.server is required for kind=external"))
                .map(External.apply)
            case Some(other) => Left(IrError.badValue(s"unknown tenant.kind '$other'"))
            case None => Left(IrError.badValue("tenant.kind is required"))

        end if

  /**
   * 信封 ↔ 租户一致性（§0，C40）：`human ⇒ ingress=human`；`llm ⇒ ingress ∈ {llm, script}`
   * （脚本由谁触发不改变权限主体）；`external ⇒ ingress=external`。不一致 ⇒ `router.schema.bad_value`。
   */
  def allows(t: Tenant, ingress: Ingress): Boolean = t match
    case _: Human => ingress == Ingress.Human
    case _: Llm => ingress == Ingress.Llm || ingress == Ingress.Script
    case _: External => ingress == Ingress.External

end Tenant

/**
 * 单次调用的上下文（执行期）。`node` = 节点路径（§3.3）；`root` = VFS 根（装配面注入）。
 * 计划体内**禁止**含身份/时间/随机/凭证（[I2]），需要它们的一律从此处取。
 */
final case class CallCtx(
  tenant: Tenant,
  ingress: Ingress,
  sessionId: String,
  requestId: String,
  node: String,
  root: VfsRoot,
  safety: Safety,
  limits: IrLimits = IrLimits.default,
  /** 本节点的 stdin 数据面（管道上游 / `stdin` 字面量 / `Redirect(in)` 文件），执行器注入。 */
  stdin: Option[StreamValue] = None,
  /** 本节点**已 canonical 化**的路径参数（[P8]：判定与执行共用同一套 canonical 逻辑）。 */
  pathArgs: Map[String, VfsPath.Canon] = Map.empty
):
  /** 路径参数的唯一解析单点（canonical → 宿主）。 */
  def resolveArg(canon: VfsPath.Canon): os.Path = VfsPath.resolve(root, canon)

  /** 取已声明的路径参数（缺省 ⇒ None；`dev:fs:*` 以「无 path」为「读 stdin / 列根」。 */
  def argPath(key: String): Option[os.Path] = pathArgs.get(key).map(resolveArg)

  def withNode(p: String): CallCtx = copy(node = p)
end CallCtx
