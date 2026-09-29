package nebflow.ir

import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Json, JsonObject}

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
 * `ir.policy.rules` 配置解码（P1-4 策略表数据化，fail-closed）。
 *
 * 格式 = [[PolicyRule]] 的 JSON 镜像（`nebflow.json` 顶层 `ir.policy.rules`，与
 * P1-3 的 `ir.llmIngress` 同节）。严格区上沿到 **policy 节级**：policy 存在 ⇒
 * 必须恰为 `{"rules":[…]}` 形——任何 typo 路径（`rulez`、缺 `rules`、非对象…）都
 * 落到「拒」而非静默当作无规则（静默回落会丢用户 Deny = 明文放宽，红线）。
 * 深层 null=删键语义（`ConfigService.mergeConfig`）在严格区内**不适用**：删规则请
 * 删整个 policy 节或写 `"rules":[]`（写面批须遵守）。
 */
object PolicyRule:

  /** 规则条目级键集：未知键 ⇒ 拒（绝不明文放宽）。 */
  private val Keys = Set("name", "decision", "reason", "rule", "tenant", "capKinds")

  /** cap 词表单源 = [[PolicyCapVocab.knownCapKinds]]（六词）。 */
  private val CapVocab: Set[String] = PolicyCapVocab.knownCapKinds

  /** 规则上限（超 ⇒ 整配置拒）。 */
  val MaxRules = 256

  /** 匹配键（重复检查用）：原始 name 串 + tenant + capKinds 集三元组。 */
  private type MatchKey = (String, Option[TenantKind], Set[String])
  private def matchKey(r: PolicyRule): MatchKey = (r.name, r.tenant, r.capKinds)

  /** `ir.policy` 节级严检。错误串一律带 `ir.policy` 前缀路径。 */
  def decodeConfigList(policy: Json): Either[String, List[PolicyRule]] =
    policy.asObject match
      case None => Left("ir.policy: must be a JSON object")
      case Some(o) =>
        o.keys.find(_ != "rules") match
          case Some(k) => Left(s"ir.policy.$k: unknown key (the policy node accepts exactly 'rules')")
          case None =>
            o("rules") match
              case None =>
                Left("""ir.policy: 'rules' is required (an empty rule set must be written as {"rules":[]})""")
              case Some(v) if v.isNull =>
                Left("ir.policy.rules: must be an array (null is not an empty rule set)")
              case Some(v) =>
                v.asArray match
                  case None => Left("ir.policy.rules: must be an array")
                  case Some(items) =>
                    if items.length > MaxRules then
                      Left(s"ir.policy.rules: ${items.length} rules exceed the limit of $MaxRules")
                    else
                      items.toList.zipWithIndex
                        .traverse((item, i) => decodeConfig(item, s"ir.policy.rules[$i]"))
                        .flatMap(checkDuplicates)

  /** 条目级严检：未知键 / decision 三值 / ask·deny 必带 reason+rule / allow 禁带 / name 模式 / capKinds 词表 / tenant 三值。 */
  def decodeConfig(json: Json, path: String): Either[String, PolicyRule] =
    json.asObject match
      case None => Left(s"$path: must be a JSON object")
      case Some(o) =>
        o.keys.find(!Keys.contains(_)) match
          case Some(k) => Left(s"$path.$k: unknown key")
          case None =>
            for
              name <- string(o, "name", path).flatMap(checkName(_, s"$path.name"))
              decision <- decodeDecision(o, path)
              tenant <- decodeTenant(o, path)
              capKinds <- decodeCapKinds(o, path)
            yield PolicyRule(name, decision, tenant, capKinds)

  private def string(o: JsonObject, key: String, path: String): Either[String, String] =
    o(key).flatMap(_.asString).filter(_.nonEmpty).toRight(s"$path.$key: required (non-empty string)")

  /**
   * name 模式校验：`*` 只许单个且必在末位（通配语义 = [[PolicyRule.matches]] 的
   * startsWith）；把 `*` 换 `x` 后必须过 [[Names.syntax]]——既拦中段 star 也拦
   * 非法段/超长/单段名。
   */
  private def checkName(name: String, path: String): Either[String, String] =
    val stars = name.count(_ == '*')
    if stars > 1 || (stars == 1 && !name.endsWith("*")) then
      Left(s"$path: '*' is only allowed as a single trailing wildcard")
    else
      val probe = name.map(c => if c == '*' then 'x' else c)
      Names.syntax(probe).left.map(err => s"$path: ${err.message}").map(_ => name)

  /** ask/deny 必带非空 reason+rule（§8.6 审计面）；allow 禁带（带 = 拼写嫌疑，拒）。 */
  private def decodeDecision(o: JsonObject, path: String): Either[String, Decision] =
    o("decision").flatMap(_.asString).toRight(s"$path.decision: required (allow|ask|deny)").flatMap {
      case "allow" =>
        if o("reason").isDefined || o("rule").isDefined then
          Left(s"$path.decision: 'allow' must not carry 'reason'/'rule' (likely a misspelled ask/deny rule)")
        else Right(Decision.Allow)
      case wire @ ("ask" | "deny") =>
        string(o, "reason", path).flatMap(reason =>
          string(o, "rule", path)
            .map(rule => if wire == "ask" then Decision.Ask(reason, rule) else Decision.Deny(reason, rule))
        )
      case other => Left(s"$path.decision: must be one of allow|ask|deny (got '$other')")
    }

  private def decodeTenant(o: JsonObject, path: String): Either[String, Option[TenantKind]] =
    o("tenant") match
      case None => Right(None)
      case Some(v) =>
        v.asString.flatMap(decodeKind) match
          case Some(k) => Right(Some(k))
          case None => Left(s"$path.tenant: must be one of human|llm|external")

  private def decodeKind(raw: String): Option[TenantKind] = raw match
    case "human" => Some(TenantKind.Human)
    case "llm" => Some(TenantKind.Llm)
    case "external" => Some(TenantKind.External)
    case _ => None

  private def decodeCapKinds(o: JsonObject, path: String): Either[String, Set[String]] =
    o("capKinds") match
      case None => Right(Set.empty)
      case Some(v) =>
        v.asArray match
          case None => Left(s"$path.capKinds: must be an array of cap kinds")
          case Some(items) =>
            items.zipWithIndex
              .traverse((item, i) =>
                item.asString match
                  case Some(k) if CapVocab.contains(k) => Right(k)
                  case Some(k) =>
                    Left(s"$path.capKinds[$i]: unknown cap kind '$k' (known: ${CapVocab.toList.sorted.mkString(", ")})")
                  case None => Left(s"$path.capKinds[$i]: must be a string")
              )
              .map(_.toSet)

  /**
   * 重复匹配键检查：同 `(name, tenant, capKinds)` 两条 ⇒ 整配置拒——`rules.find`
   * 首匹配语义下后条必死（Deny 被前条遮蔽 = 放宽向量）。不同键的重叠模式
   * （精确名 + 通配）合法 = 特例压通例的既有分层用法，不拒。
   */
  private def checkDuplicates(rules: List[PolicyRule]): Either[String, List[PolicyRule]] =
    val seen = scala.collection.mutable.LinkedHashMap.empty[MatchKey, Int]
    var dup: Option[String] = None
    rules.zipWithIndex.foreach { case (r, i) =>
      if dup.isEmpty then
        seen.get(matchKey(r)) match
          case Some(j) =>
            dup = Some(
              s"ir.policy.rules: duplicate matching key (name='${r.name}', " +
                s"tenant=${r.tenant.map(_.toString).getOrElse("-")}, capKinds={${r.capKinds.toList.sorted.mkString(", ")}}) " +
                s"at rules[$j] and rules[$i] — first-match semantics would shadow the later rule"
            )
          case None => seen.update(matchKey(r), i)
    }
    dup.toLeft(rules)
  end checkDuplicates

end PolicyRule

/**
 * cap 词表单源（零出边叶节点）：引擎认识的六词**唯一定义处**——[[PolicyConfig]] 的缺省
 * `knownCapKinds` 与 [[PolicyRule]] 的解码词表共用此份（原先由后者经
 * `PolicyConfig().knownCapKinds` 间接取值，令同文件两个顶层节点互相引用）。本对象只含
 * 字面量、不引用任何其它节点，故结构上不可能参与环。
 */
private[ir] object PolicyCapVocab:

  val knownCapKinds: Set[String] = Set("FsRead", "FsWrite", "Net", "Exec", "Secret", "MemoryWrite")

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
  knownCapKinds: Set[String] = PolicyCapVocab.knownCapKinds,
  rules: List[PolicyRule] = Nil,
  dangerousBash: (String, JsonObject) => Boolean = (_, _) => true,
  /** 与 `dangerousBash` 合成的非 `RealBash` 命令名集（批 C 组合面；默认 ∅ = 不扩）。 */
  dangerousBashNames: Set[String] = Set.empty,
  /** 无规则命中时的缺省：纯读命令放行，其余（写/网/执行/凭证）缺省 `Ask`。 */
  defaultDenyUnknownTenant: Boolean = false,
  /**
   * P1-4 fail-closed：`ir.policy` 配置被拒时的整体拒答理由。`Some` ⇒ [[PolicyEngine.decide]]
   * 一切判定 `Deny`（rule=`policy:config-rejected`，exit 125）——**绝不静默回落基线**
   * （回落会丢用户 Deny = 明文放宽）。默认 `None` = 零行为差。
   */
  denyAll: Option[String] = None
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
    config.denyAll match
      // P1-4 fail-closed 最先查（先于 unknown-cap 与一切规则匹配）：配置被拒 ⇒ 整体拒答，
      // 修好即恢复（boot best-effort 存活，先例 ToolLoader 无效 tool.json 跳过不炸）。
      case Some(reason) =>
        Decision.Deny(
          s"the ir.policy configuration was rejected — everything denies until it is fixed: $reason",
          "policy:config-rejected"
        )
      case None =>
        decideWithRules(tenant, ingress, target, safety)

  private def decideWithRules(tenant: Tenant, ingress: Ingress, target: NodeTarget, safety: Safety): Decision =
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

  end decideWithRules

  /**
   * 请求级视图（P1-4，纯函数）：用户规则**前置**（`extra ++ config.rules`，首匹配
   * 先中 ⇒ 用户精确 `Allow` 仍是唯一真写放行通道、用户 `Deny` 是任何档位都翻不过的
   * 硬底）；`failing` = 配置被拒的首错路径 ⇒ denyAll 视图（全判定 `Deny`，绝不静默
   * 回落基线）。同参两次派生 ⇒ 判定一致。
   */
  def forRequest(extra: List[PolicyRule], failing: Option[String] = None): PolicyEngine =
    new PolicyEngine(config.copy(rules = extra ++ config.rules, denyAll = failing))

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
