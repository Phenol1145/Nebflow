package nebflow.ir

import io.circe.syntax.*
import io.circe.JsonObject

/** 判定三态（§8.2）。`Allow` 无附注；`Ask`/`Deny` 带人类可读 reason + 命中的规则名。 */
enum Decision:
  case Allow
  case Ask(reason: String, rule: String)
  case Deny(reason: String, rule: String)

object Decision:

  def wire(d: Decision): String = d match
    case Allow => "allow"
    case _: Decision.Ask => "ask"
    case _: Decision.Deny => "deny"

  def ruleOf(d: Decision): Option[String] = d match
    case Allow => None
    case Decision.Ask(_, rule) => Some(rule)
    case Decision.Deny(_, rule) => Some(rule)

  /** 合成：任一 `Deny` ⇒ `Deny`；任一 `Ask` ⇒ `Ask`（§8.2「caps 与档位任一要求 Ask ⇒ Ask」）。 */
  def combine(a: Decision, b: Decision): Decision = (a, b) match
    case (d: Decision.Deny, _) => d
    case (_, d: Decision.Deny) => d
    case (a: Decision.Ask, _) => a
    case (_, b: Decision.Ask) => b
    case _ => Allow

end Decision

/**
 * 策略表一行（§8.2 的 `(tenantProfile, cmdPattern, caps) → Allow|Ask|Deny`）。
 * `name` 支持精确匹配或单尾通配（`dev:fs:*`）；`capKinds` 为空 = 不限。
 */
final case class PolicyRule(
  name: String,
  decision: Decision,
  tenant: Option[TenantKind] = None,
  capKinds: Set[String] = Set.empty
):

  def matches(t: Tenant, target: NodeTarget): Boolean =
    val tenantOk = tenant.forall(_ == Tenant.kind(t))
    val nameOk =
      if name.endsWith("*") then target.command.startsWith(name.dropRight(1)) else target.command == name
    val capsOk = capKinds.isEmpty || target.capKinds.forall(capKinds.contains)
    tenantOk && nameOk && capsOk
end PolicyRule

/**
 * 策略引擎配置（§8.1/§8.2）。
 *
 * `knownCapKinds` 是**引擎认识**的 cap 词表：不认识 ⇒ `Deny`（fail-closed，§8.1）。
 * `dangerousBash` 是既有危险判定的注入点（先例 `core.permissions.ToolReversibility`
 * / `BashTool.dangerLevel`）——`RealBash` 绑定命令**必须**与它合成（§8.2）；默认
 * fail-closed 为「一律危险」，装配面注入真判定后才可能放行。
 *
 * `dangerousBashNames`（批 C，P1-2）：`RealBash` 之外的**bash 面命令名集**——引擎
 * 无法从 `Binding.Dev` 辨认「这是个 shell」，装配面把桥接 bash（如 `dev:tool:bash`）
 * 的 IR 名注入此集，`decide()` 即对它与 `RealBash` 同款合成危险判定。默认 `Set.empty`
 * ⇒ 引擎行为与扩展前逐字节一致（`RealBash` 语义不动）。
 */
final case class PolicyConfig(
  knownCapKinds: Set[String] = Set("FsRead", "FsWrite", "Net", "Exec", "Secret", "MemoryWrite"),
  rules: List[PolicyRule] = Nil,
  dangerousBash: (String, JsonObject) => Boolean = (_, _) => true,
  /** 与 `dangerousBash` 合成的非 `RealBash` 命令名集（批 C 组合面；默认 ∅ = 不扩）。 */
  dangerousBashNames: Set[String] = Set.empty,
  /** 无规则命中时的缺省：纯读命令放行，其余（写/网/执行/凭证）缺省 `Ask`。 */
  defaultDenyUnknownTenant: Boolean = false
)

/**
 * 策略引擎（§8.2）。判定输入 = **IR 数据 + 声明 caps + 租户/ingress + 安全档**；
 * **禁止**把模型自由文本或未解析字符串作为判定输入（从根上消灭「shell 解析歧义绕过」
 * 一类漏洞）——本引擎的输入面只有 [[NodeTarget]]（来自已解码、已 canonical 化的计划）。
 *
 * 与既有安全档的合成（落实 V2 红线 5，[D24]）：
 *  - 档位**可以**把 `Ask` 降为 `Allow`（这正是 `auto-all` 的既有语义）；
 *  - **禁止**把 `Deny` 升为 `Allow` —— caps 判定是**硬底**；
 *  - `binding=RealBash` 的命令**必须**与既有危险判定合成（此处注入 `dangerousBash`）；
 *    批 C（P1-2）起，名 ∈ `dangerousBashNames` 的命令（桥接 bash 面）同款合成。
 */
final class PolicyEngine(val config: PolicyConfig):

  /**
   * 逐节点判定。**执行前**对整计划跑完（[I7]）——判定的短路规则见 [[Router]]：
   * 遇首个 `Deny` 即停判并整计划拒绝（v1 取值域最小，§8.4）。
   */
  def decide(tenant: Tenant, ingress: Ingress, target: NodeTarget, safety: Safety): Decision =
    val unknownCap = target.capKinds.filterNot(config.knownCapKinds.contains)
    if unknownCap.nonEmpty then
      Decision.Deny(
        s"cap kind not recognized by the policy engine: ${unknownCap.toList.sorted.mkString(", ")}",
        "fail-closed:unknown-cap"
      )
    else
      prefixViolation(target) match
        case Some((capRendered, path)) =>
          Decision.Deny(
            s"path '$path' is outside the declared cap prefix $capRendered ([P7]: exists but not permitted)",
            "cap:prefix"
          )
        case None =>
          val base = config.rules.find(_.matches(tenant, target)) match
            case Some(rule) => rule.decision
            case None =>
              if target.capKinds.subsetOf(Set("FsRead")) then Decision.Allow
              else Decision.Ask("no matching policy rule for a capability-bearing command", "default:no-rule")
          val withBash =
            // 批 C（P1-2）：合成面从「仅 RealBash」扩为「RealBash ∨ 名 ∈ dangerousBashNames」
            // ——桥接 bash（Binding.Dev）由此接通同一危险底座。合成只升不降（combine 的
            // 既有语义：Deny 恒 Deny；Allow+危险 ⇒ Ask），Ask 字面量逐字不动。
            val bashFace = target.binding.exists(_.isInstanceOf[Binding.RealBash]) ||
              config.dangerousBashNames.contains(target.command)
            if bashFace && config.dangerousBash(target.command, target.args) then
              Decision.combine(base, Decision.Ask("bash command is not provably safe", "bash:danger"))
            else base
          composeSafety(withBash, safety)

    end if

  end decide

  /**
   * caps ↔ 安全档合成（§8.2）：档位可把 `Ask` 降为 `Allow`，**禁止**把 `Deny` 升为 `Allow`。
   * 另：`backend 与命令自控 + 策略表` 是硬底 —— 即使 `auto-all`，`Deny` 依旧 `Deny`。
   */
  private def composeSafety(d: Decision, safety: Safety): Decision = d match
    case _: Decision.Deny => d
    case _: Decision.Ask if safety == Safety.AutoAll => Decision.Allow
    case other => other

  /**
   * cap 前缀匹配（§8.1/§4.6 P7）：判定对象 = 由 `pathArgs` 声明的位置，canonical 化后
   * **按段**比较（[P6]/[D16]：字符串前缀匹配是经典绕过）。越 prefix ⇒ 策略拒绝（exit 125）。
   */
  private def prefixViolation(target: NodeTarget): Option[(String, String)] =
    val prefixes = target.caps.toList.collect {
      case Cap.FsRead(p) => ("FsRead", p)
      case Cap.FsWrite(p) => ("FsWrite", p)
    }
    prefixes.iterator
      .flatMap { case (kind, raw) =>
        val outside = VfsPath.canonPrefix(raw) match
          case Right(prefix) => target.pathArgs.values.filterNot(VfsPath.isWithin(prefix, _))
          // 声明本身非法（注册期已拦，这里 fail-closed 兜底）
          case Left(_) => target.pathArgs.values.toList
        outside.headOption.map(c => (s"$kind($raw)", c.render))
      }
      .nextOption()

  /** §7.4 的三处检查点之一：派发时（纵深防御）与 lowering 时都要拦不可见面。 */
  def checkAudience(ingress: Ingress, cmd: CommandDef): Either[IrError, Unit] =
    val want = Audience.of(ingress)
    if cmd.audiences.contains(want) then Right(())
    else
      Left(
        IrError
          .invalidArgs(s"command '${cmd.name}' is not visible to the ${Ingress.wire(ingress)} ingress (§7.4)")
          .withDetail("reason", "audience".asJson)
          .withDetail("required", cmd.audiences.map(_.toString).toList.sorted.asJson)
      )

end PolicyEngine
