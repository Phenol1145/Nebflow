/* 命令 IR 路由层 P1-4（策略表数据化）：`nebflow.json` 顶层 `ir.policy.rules` 的逐访问
 * 热读 + 请求级策略视图派生（已批准方案裁定）。
 *
 *  - 加载时机 = 每请求热读（镜像 core.GlobalSafety.defaultMode：每次访问 IO.blocking
 *    重读、无缓存——免重启即生效；不为它造第二个 watcher）。请求粒度：一次 submit 只
 *    load 一次，Planner/Executor/audit 用同一视图（消解 §8.4 先全判后执行中的节点间
 *    规则漂移，与 capsDigest 防描述符漂移同理）。
 *  - 降级口径（fail-closed）三分道：缺文件/缺 ir 节/policy 为 null ⇒ Right(Nil)=无用户
 *    规则，判定与昨天逐字节一致；policy 存在但任一非法（节级/rule 级/重复匹配键/>256/
 *    文件不可解析/读盘失败）⇒ 整配置拒绝一条不采 ⇒ denyAll 视图（本请求所有判定
 *    Deny，rule=policy:config-rejected，经 policy.denied exit 125 呈现）+ ERROR 日志
 *    （去重：仅错误串变化时重打）；**绝不静默回落基线**——回落会丢用户 Deny（如
 *    Deny(ext:*) 因 rulez typo 失效 ⇒ 落缺省 Ask 甚至被 auto-all 折 Allow）= 明文放宽。
 *  - ir 节点级容忍未知兄弟键（口径钉死：shared/config.scala 的 `Option[Json]` 原样
 *    透传 + IrLlmRoute.llmIngressEnabled 宽读是现状，本批不改 NebflowServiceConfig
 *    解码面）：残余 typo 向量（ir.policies）以 WARN 留痕不拒（零行为影响；留痕去重
 *    同 ERROR）。不对称的理据：开关类坏值回安全缺省=关；数据类必须整体采或不采。
 */
package nebflow.gateway

import cats.effect.IO
import io.circe.Json
import nebflow.ir.*
import nebflow.shared.{NebflowLogger, PathUtil}

private[gateway] object IrPolicyRules:

  private val logger = NebflowLogger.forName("nebflow.ir.policy")

  /** ir 节内已被 P1-3/P1-4 认领的键；其余兄弟键 WARN 留痕（ir 级 typo 向量，不拒）。 */
  private val IrKnownKeys = Set("llmIngress", "policy")

  /** ERROR/WARN 留痕去重（@volatile：良性竞态，最坏重复一条）。 */
  @volatile private var lastError: String = null
  @volatile private var lastWarn: String = null

  /** 每访问热读（先例 GlobalSafety.defaultMode）：缺文件 ⇒ Right(Nil)；非法/读失败 ⇒ Left(首错路径)。 */
  def load: IO[Either[String, List[PolicyRule]]] =
    IO.blocking {
      val configPath = PathUtil.configJsonReadPath(PathUtil.dataRoot)
      if !os.exists(configPath) then Right(Nil)
      else
        io.circe.parser.parse(os.read(configPath)) match
          case Left(err) => Left(s"nebflow.json is not parsable: ${err.getMessage}")
          case Right(json) => fromConfig(json)
    }.handleErrorWith(e => IO.pure(Left(s"nebflow.json read failed: ${Option(e.getMessage).getOrElse(e.toString)}")))

  /** 纯解码段：ir 节 focus（None/Null ⇒ Right(Nil)）+ 兄弟键 WARN + 节级严检。 */
  def fromConfig(json: Json): Either[String, List[PolicyRule]] =
    json.hcursor.downField("ir").focus match
      case None => Right(Nil)
      case Some(node) =>
        if node.isNull then Right(Nil)
        else
          node.asObject match
            case Some(o) =>
              o.keys
                .find(!IrKnownKeys.contains(_))
                .foreach(k =>
                  warnOnce(s"ir config node has unknown key '$k' (known: llmIngress, policy) — tolerated, not rejected")
                )
              o("policy") match
                case None => Right(Nil)
                case Some(p) if p.isNull => Right(Nil)
                case Some(p) => PolicyRule.decodeConfigList(p)
            case None =>
              warnOnce(
                "ir config node is not a JSON object — lenient read (llmIngress fail-safe off, no policy rules apply)"
              )
              Right(Nil)

  /** 请求级视图：Right ⇒ 用户规则前置（base.forRequest）；Left ⇒ denyAll + ERROR 去重。 */
  def view(base: PolicyEngine): IO[PolicyEngine] =
    load.map {
      case Right(rules) => base.forRequest(rules)
      case Left(reason) =>
        errorOnce(s"ir policy config rejected — all decisions deny until it is fixed: $reason")
        base.forRequest(Nil, Some(reason))
    }

  private def warnOnce(msg: String): Unit =
    if lastWarn != msg then
      lastWarn = msg
      logger.warnSync(msg)

  private def errorOnce(msg: String): Unit =
    if lastError != msg then
      lastError = msg
      logger.errorSync(msg)

end IrPolicyRules
