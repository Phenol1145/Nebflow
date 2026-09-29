/* 命令 IR 路由层 P1-2 批 A：内置 Tool → IR 命令桥接的**集中声明表**（唯一手工元数据面）。
 *
 * 层位：桥必须同时看见 `nebflow.ir`（CommandDef/Cap）与 `nebflow.core.tools`
 * （ToolRegistry/RemoteExecutor），故落 gateway（层序 `shared < actor < ir < core < … < gateway`，
 * 先例 `WsIrHandlers.scala` 同款双侧视野）。
 *
 * 本文件只声明**手工裁定**的部分：IR 名、caps、Ask 策略规则。其余描述符字段由
 * `IrToolBridge` 从工具定义**机械装配**（argsSchema/params 取 augmentSchema 后的
 * inputSchema，pathArgs 恒空，audiences={Human}，llmName=旧 Tool 名——§7.5 迁移期锚）。
 *
 * caps 纪律（§8.1「声明替代静态分析」，全批无既有元数据可继承，逐件手工声明）：
 *  - 文件面工具的绝对路径语义（如 `ReadTool` 的 `file_path`）与 VFS canon
 *    （`ir/Vfs.scala` 拒盘符/UNC/`~`）正面冲突 ⇒ `pathArgs = ∅` + 前缀 `"*"`
 *    （VFS 之外的不透明宿主访问，§8.1 明文形态）。`pathArgs = ∅` ⇒ 前缀包含性
 *    判定恒空（`PolicyEngine.prefixViolation` 无对象可判）⇒ 判定诚实交给策略面
 *    （下方 askRule 一律 `Ask`，不假装能判）。
 *  - 凡经 `RgHelper` 起 rg 子进程的件（Glob/Grep，`new ProcessBuilder(...).start()`）
 *    必带 `Exec`——与 §8.1「Exec = 生成子进程」同机制同口径；漏报会使 Glob/Grep 在
 *    缺省策略下分叉（FsRead-only 走 Allow、带 Exec 走 Ask，`ir/Policy.scala` 缺省分支）。
 */
package nebflow.gateway

import nebflow.ir.{Audience, Cap, CommandIo, Decision, PolicyRule, StreamKind, Trust}

/**
 * 一件桥接命令的手工声明：IR 名（`dev:tool:<lower(旧名)>`）、旧 Tool 名（ToolRegistry
 * 单键 + §7.5 `llmName` 锚）、人面一句话描述、caps 精确集。params **不在**此表——
 * 由 `IrToolBridge` 从 augmentSchema 后的 inputSchema 机械生成（全 named 无 positional）。
 */
final case class Bridged(irName: String, toolName: String, description: String, caps: Set[Cap])

object IrToolCaps:

  /** 批 A 七件（只读面为主；批 B 写面 / 批 C 高能力面另批）。 */
  val bridged: List[Bridged] = List(
    Bridged(
      irName = "dev:tool:read",
      toolName = "Read",
      description = "Read a host file (cat -n style, absolute path) — bridged builtin Read tool.",
      caps = Set(Cap.FsRead("*"))
    ),
    Bridged(
      irName = "dev:tool:glob",
      toolName = "Glob",
      description = "Fast glob file matching via a ripgrep subprocess — bridged builtin Glob tool.",
      // Exec：实现走 RgHelper 起 rg 子进程（GlobTool → runRg → ProcessBuilder.start）
      caps = Set(Cap.FsRead("*"), Cap.Exec)
    ),
    Bridged(
      irName = "dev:tool:grep",
      toolName = "Grep",
      description = "Regex content search via a ripgrep subprocess — bridged builtin Grep tool.",
      // Exec：同 Glob——rg 子进程承载搜索
      caps = Set(Cap.FsRead("*"), Cap.Exec)
    ),
    Bridged(
      irName = "dev:tool:nodelist",
      toolName = "NodeList",
      // FsRead：读宿主侧项目 flowmap 存储（ProjectRuntimeRegistry → FlowMapStore）
      caps = Set(Cap.FsRead("*")),
      description = "List a mounted project's Flow Map snapshot — bridged builtin NodeList tool."
    ),
    Bridged(
      irName = "dev:tool:nodecancel",
      toolName = "NodeCancel",
      // FsWrite：节点终态化写宿主存储（cancel/reap 落 store）
      caps = Set(Cap.FsWrite("*")),
      description = "Cancel a running Flow Map node — bridged builtin NodeCancel tool."
    ),
    Bridged(
      irName = "dev:tool:node_report",
      toolName = "node_report",
      // FsWrite：追加宿主 node-reports.jsonl（NodeReportRegistry.register 持久化腿）
      caps = Set(Cap.FsWrite("*")),
      description = "Declare a node's terminal semantics (node sessions only) — bridged node_report tool."
    ),
    Bridged(
      irName = "dev:tool:listfriends",
      toolName = "ListFriends",
      // Net：withClient 网络调用（FriendService.listFriends → NeblinkClient）
      caps = Set(Cap.Net("*")),
      description = "List the user's NebLink friends (read-only roster) — bridged builtin ListFriends tool."
    )
  )

  /** 全批共用管道契约：不吃 stdin、产出 text（J6：适配器只产 `StreamValue.Text`）。 */
  val io: CommandIo = CommandIo(stdin = None, stdout = StreamKind.Text)

  /** 七件统一由桥装配的字段（specs 的表驱动断言面，见 `IrToolBridge.toCommandDef`）。 */
  val pathArgs: Set[String] = Set.empty
  val audiences: Set[Audience] = Set(Audience.Human)
  val trust: Trust = Trust.Builtin

  /**
   * 策略面（§8.1 SHOULD 的落地）：`dev:tool:*` 一律 `Ask`——七件全为 `prefix="*"` 的
   * VFS 外不透明访问，包含性判定不可做就不假装能判。规则按名前缀只罩 `dev:tool:*`
   * ⇒ `dev:fs:*` 的缺省行为（纯读放行）不变。`auto-all` 档把 `Ask` 折 `Allow` 是既有
   * 档位语义（`ir/Policy.scala` composeSafety）；即便摘掉本规则，Glob/Grep 的 `Exec`
   * 也使缺省走 `Ask`（缺省分支），词表与兜底双保险。
   */
  val askRule: PolicyRule = PolicyRule(
    name = "dev:tool:*",
    decision = Decision.Ask(
      "bridged tool touches the host outside the VFS (opaque host access, prefix=\"*\" cannot be inclusion-checked)",
      "bridge:opaque-host"
    )
  )

  def askRules: List[PolicyRule] = List(askRule)

end IrToolCaps
