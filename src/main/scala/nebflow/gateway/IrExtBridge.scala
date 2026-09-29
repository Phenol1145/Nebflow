/* 命令 IR 路由层 P1-4（ext: 过渡注册）：ScriptTool 三层装载（global/team/flow，
 * ToolLoader.loadAll）→ IR 命令的**声明级**桥。过渡形态（§7.2/§12:637「待 P1 定档」）：
 * **注册面存在、执行面诚实拒答**——`Binding.Node` 的扩展子进程执行宿主落地后本桥只补
 * 执行腿，描述符面不变（先例：批 A/C 的 ToolContext 诚实缺席——确定性拒答优于假门）。
 *
 * 数据面 = `(ExternalToolConfig, os.Path)` 对（loadAll 同型）：`ScriptTool` 的 config
 * 是私有构造参（实例面拿不到 command/layer），而 ExternalToolConfig 恰含全部声明级
 * 字段（name/description/command/inputSchema/layer/scope）——桥与钩都不触碰
 * ScriptTool 类型面（ScriptTool.scala 零改动）。
 *
 * 桥纪律（同 IrToolBridge）：零 ToolRegistry 读写；ext:/dev: 名绝不进
 * TOOL_MAP/ALL_TOOLS；IrLlmRoute.llmNameToIr 只从 IrToolCaps.bridged 静态构建 ⇒
 * ext: 不进 llm 改道表（P1-3 开关 on 也不改模型面行为）。
 */
package nebflow.gateway

import nebflow.core.tools.ExternalToolConfig
import nebflow.ir.*
import nebflow.shared.NebflowLogger

private[gateway] object IrExtBridge:

  private val logger = NebflowLogger.forName("nebflow.ir.extbridge")

  /** ext: 桥已注册名集（自记账：ext: 名集动态，幂等 reindex = 先摘全部旧名再逐件挂）。 */
  private val registered = java.util.Collections.newSetFromMap(
    new java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean]()
  )

  /** 过渡档 caps：`sh -c` 任意宿主执行 ⇒ 最保守四件套（§8.1 诚实声明；pathArgs=∅ 同 IrToolCaps 先例）。 */
  val caps: Set[Cap] = Set(Cap.Exec, Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Net("*"))

  /** §7.4 ↔ ToolLoader.layerPriority 对齐：global=User、team/flow=Project；未知层 fail-closed 跳过。 */
  def trustOf(layer: String): Option[Trust] = layer match
    case "global" => Some(Trust.User)
    case "team" | "flow" => Some(Trust.Project)
    case _ => None

  /**
   * 幂等 reindex（先摘后挂）：boot（GatewayMain.loadExternalTools 里 reload 内的钩）与
   * watcher 热重载**同一入口**。单件失败（IR 名非法/同档 N4 碰撞/未知层）记 ERROR 跳过
   * 该件、先注册者留（确定性 = ToolLoader merge 输出序：层优先级升序、高层后写覆盖同
   * 原始名）；跨档覆盖（replaced=true）WARN 留痕。表操作由 ir.CommandRegistry 的
   * synchronized 保护（本批引入 watcher 纤程写表）。
   */
  def reindex(registry: CommandRegistry, configs: List[(ExternalToolConfig, os.Path)]): Unit =
    registered.forEach(name => registry.unregister(name))
    registered.clear()
    configs.foreach((config, _) => registerOne(registry, config))

  def registerOne(registry: CommandRegistry, config: ExternalToolConfig): Unit =
    val irName = s"ext:tool:${Names.normalize(config.name)}"
    Names.syntax(irName) match
      case Left(err) =>
        logger.errorSync(
          s"ext bridge: skipping '${config.name}' (${config.layer} layer): IR name '$irName' is invalid: ${err.message}"
        )
      case Right(()) =>
        trustOf(config.layer) match
          case None =>
            logger.errorSync(s"ext bridge: skipping '${config.name}': unknown layer '${config.layer}'")
          case Some(trust) =>
            registry.register(toCommandDef(config, trust)) match
              case Left(err) =>
                logger.errorSync(s"ext bridge: registering $irName failed: ${err.message} — skipped")
              case Right(reg) =>
                registered.add(reg.name)
                if reg.replaced then
                  logger.warnSync(
                    s"ext bridge: $reg.name (${config.layer} layer) replaced a lower-trust registration (§7.4)"
                  )

    end match

  end registerOne

  /** 一份声明级配置 → `CommandDef`（字段来源见行内注释；名字 = `ext:tool:` + 规范化原名）。 */
  private def toCommandDef(config: ExternalToolConfig, trust: Trust): CommandDef =
    CommandDef(
      // N3/N4：IR 名 = ext:tool: + Names.normalize(原始名)（确定性；原始 name 与 sh -c
      // 模板原文存 binding——IR 名到实现的双射由 binding 保证，禁字符串反解）
      name = s"ext:tool:${Names.normalize(config.name)}",
      description = config.description,
      argsSchema = config.inputSchema,
      binding = Binding.Node(extension = config.name, entry = config.command),
      // 声明级管道契约：不吃 stdin、产出 text（执行宿主落地前的保守声明）
      io = CommandIo(stdin = None, stdout = StreamKind.Text),
      pathArgs = Set.empty,
      caps = caps,
      // §9 红线 4：ext: 的 params 恒空（外部面只有 --json；Registry.validateParams 强制）
      params = Nil,
      delivery = Delivery.Both,
      trust = trust,
      // 过渡档 {Human}：真执行面落地时翻 {Human,Llm} + llmName 进改道表（§7.4 终态由 P2 定档）
      audiences = Set(Audience.Human),
      // §7.5 迁移期锚 = 旧 ToolRegistry 键原样；audiences 不含 Llm ⇒ 校验面零触发，纯元数据锚
      llmName = Some(config.name)
    )

end IrExtBridge
