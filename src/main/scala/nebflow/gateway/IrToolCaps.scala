/* 命令 IR 路由层 P1-2 批 A（只读七件）+ 批 B（写面八件）+ 批 C（高能力面十五件）：
 * 内置 Tool → IR 命令桥接的**集中声明表**（唯一手工元数据面）。
 *
 * 层位：桥必须同时看见 `nebflow.ir`（CommandDef/Cap）与 `nebflow.core.tools`
 * （ToolRegistry/RemoteExecutor），故落 gateway（层序 `shared < actor < ir < core < … < gateway`，
 * 先例 `WsIrHandlers.scala` 同款双侧视野）。
 *
 * 本文件只声明**手工裁定**的部分：IR 名、caps、Ask 策略规则。其余描述符字段由
 * `IrToolBridge` 从工具定义**机械装配**（argsSchema/params 取 augmentSchema 后的
 * inputSchema，pathArgs 恒空，P1-3 起 audiences={Human,Llm}，llmName=旧 Tool 名——
 * §7.5 迁移期锚，见 [[audiences]] 行内注）。
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
 *    批 B 同口径：NodeEdit 的 worktree=true 创建腿起 git 子进程 ⇒ 亦带 `Exec`。
 *  - 批 B 写面诚实声明：Write/Edit/ProjectCreate/NodeEdit/TaskList/TaskBoard 写宿主
 *    任意绝对路径/存储根——桥模板 `sandbox=off`（`FileSandbox.checkWrite` 的 pathRoot
 *    为空直接放行）⇒ 无包含性判定可做，一律 `prefix="*"` + askRule `Ask`；**显式
 *    Allow 规则是唯一真写放行通道**（无规则缺省分支走 `default:no-rule` Ask，零副作用）。
 *  - 批 B 恒拒答注记：Pop/TaskBoard 经桥模板 `agentDef=None`/`isDispatcher=false` ⇒
 *    身份闸 fail-closed ⇒ 恒 POP_NEBULA_ONLY / TBOARD_FORBIDDEN（先于一切副作用）——
 *    「注册面存在、执行面诚实拒答」形态（批 A node_report 先例同构）；caps 仍按
 *    工具自身能力面声明，不因拒答而缩水。
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

  /** 桥接 bash 的 IR 名单源（Bridged 行与 IrGateway 的 dangerousBashNames 装配共用）。 */
  val bashIrName: String = "dev:tool:bash"

  /** 批 A 只读七件 + 批 B 写面八件 + 批 C 高能力面十五件（AskUserQuestion 不注册）。 */
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
    ),
    // ── 批 B 写面八件（irName = dev:tool:<lower(旧名)>，机械命名规则同批 A）──────
    Bridged(
      irName = "dev:tool:write",
      toolName = "Write",
      // FsWrite("*")：写宿主任意绝对路径（模板 sandbox=off ⇒ FileSandbox.checkWrite 的
      // pathRoot 为空直接放行）——VFS 外不透明写，prefix="*" 诚实形态
      caps = Set(Cap.FsWrite("*")),
      description = "Write a host file (absolute path, full content + diff stats) — bridged builtin Write tool."
    ),
    Bridged(
      irName = "dev:tool:edit",
      toolName = "Edit",
      // 同 Write 面：old/new_string 替换后回写宿主文件；replace_all 为 boolean 属性 ⇒
      // 机械规则不进糖（IrToolBridge.admitsString）
      caps = Set(Cap.FsWrite("*")),
      description = "Replace a text span in a host file (absolute path) — bridged builtin Edit tool."
    ),
    Bridged(
      irName = "dev:tool:pop",
      toolName = "Pop",
      // FsRead：filePath 可为宿主绝对路径（Canvas 展示），亦可为 HTTP(S) URL ⇒ pathArgs=∅。
      // 桥模板 agentDef=None ⇒ 身份闸 fail-closed ⇒ 恒 POP_NEBULA_ONLY（先于路径解析/
      // 文件读/WS，零副作用）——经 IR 是确定性拒答件，caps 仍按能力面诚实声明
      caps = Set(Cap.FsRead("*")),
      description = "Open a file or URL in the Canvas panel (Nebula root only) — bridged builtin Pop tool."
    ),
    Bridged(
      irName = "dev:tool:card",
      toolName = "Card",
      // FsRead：call 本体不触 ctx，但 embedLocalFiles 解析 html 内 src/href/srcset/
      // url()/@import 并真实读宿主文件字节内联——真实读通道，声明面不能为空集
      caps = Set(Cap.FsRead("*")),
      description = "Render an HTML card (local file references embedded) — bridged builtin Card tool."
    ),
    Bridged(
      irName = "dev:tool:projectcreate",
      toolName = "ProjectCreate",
      // FsWrite：挂载腿在宿主 dataRoot 下建 project scaffold（workspace 是宿主绝对路径）
      caps = Set(Cap.FsWrite("*")),
      description = "Create and mount a project from a workspace path — bridged builtin ProjectCreate tool."
    ),
    Bridged(
      irName = "dev:tool:nodeedit",
      toolName = "NodeEdit",
      // FsWrite：写宿主项目 flow-map.json；worktree=true 创建腿起 git 子进程
      // （createWorktreeFor → os.proc("git", ...)）⇒ 子进程=Exec（承批 A Glob/Grep 口径）
      caps = Set(Cap.FsWrite("*"), Cap.Exec),
      description = "Create or edit a Flow Map node (writes flow-map.json) — bridged builtin NodeEdit tool."
    ),
    Bridged(
      irName = "dev:tool:tasklist",
      toolName = "TaskList",
      // FsWrite：存储=<dataRoot>/tasks.json + tasks-history.jsonl 全局根（非 ctx 派生）；
      // actor 派生：模板 agentDef=None ⇒ "unknown"（诚实不发明身份）
      caps = Set(Cap.FsWrite("*")),
      description = "Personal task list stored under the data root — bridged builtin TaskList tool."
    ),
    Bridged(
      irName = "dev:tool:taskboard",
      toolName = "TaskBoard",
      // FsWrite：task-board.json/task-history.jsonl 落项目 workspace。身份闸：模板
      // isDispatcher=false/flowNodeId=None ⇒ Other ⇒ 恒 TBOARD_FORBIDDEN（先于写）——
      // 确定性拒答件，caps 仍按写面诚实声明
      caps = Set(Cap.FsWrite("*")),
      description = "Project task board (dispatcher/node identity required) — bridged builtin TaskBoard tool."
    ),
    // ── 批 C 高能力面十五件（irName = dev:tool:<lower(旧名)>，机械命名规则同批 A/B；
    //     AskUserQuestion 不注册——缺 agentActorRef 槽位，两模式必然运行时失败，缺席优于假门）──
    Bridged(
      irName = bashIrName,
      toolName = "Bash",
      // 命令面不可静态判定（命令文本即任意宿主读写/网/执行）⇒ 诚实取最大四件集；
      // 危险命令组合面 = IrGateway 装配的 dangerousBash/dangerousBashNames（Policy 批 C）
      caps = Set(Cap.Exec, Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Net("*")),
      description = "Run a shell command on the host (foreground or background job) — bridged builtin Bash tool."
    ),
    Bridged(
      irName = "dev:tool:websearch",
      toolName = "WebSearch",
      caps = Set(Cap.Net("*")),
      description = "Web search via the configured engine — bridged builtin WebSearch tool."
    ),
    Bridged(
      irName = "dev:tool:webfetch",
      toolName = "WebFetch",
      caps = Set(Cap.Net("*")),
      description = "Fetch a URL and convert to text — bridged builtin WebFetch tool."
    ),
    Bridged(
      irName = "dev:tool:curl",
      toolName = "Curl",
      caps = Set(Cap.Net("*")),
      description = "HTTP(S) request to a URL (no local file face) — bridged builtin Curl tool."
    ),
    Bridged(
      irName = "dev:tool:mail",
      toolName = "Mail",
      // FsWrite 为侦察修正补入：短名腿 MailQueueStore 落 <dataRoot>/sessions/<sid>/
      // mail-queue.json、node 腿 FlowMailStore 落盘——真实写通道，诚实取最大
      caps = Set(Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Net("*")),
      description = "Send mail to an agent/device/project address — bridged builtin Mail tool."
    ),
    Bridged(
      irName = "dev:tool:sendmessage",
      toolName = "SendMessage",
      // FsWrite 为侦察修正补入：to=local 腿 os.makeDir.all + os.copy 宿主读写
      caps = Set(Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Net("*")),
      description = "Send a message/files to a friend, device, or local dir — bridged builtin SendMessage tool."
    ),
    Bridged(
      irName = "dev:tool:delegate",
      toolName = "Delegate",
      // 传递性：kernel 子代理带文件六件+Bash ⇒ 诚实取并集。agentLibrary 槽由
      // initBridge(lib) 模板注入（生产 Some ⇒ 真实可用；spec 侧 None ⇒ 确定性拒腿）
      caps = Set(Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Exec, Cap.Net("*")),
      description = "Delegate a task to the kernel agent (sub-agent with full tool face) — bridged Delegate tool."
    ),
    Bridged(
      irName = "dev:tool:subtask",
      toolName = "SubTask",
      // 传递性同 Delegate（自克隆语义的子代理全工具面）。执行面：模板 agentDef=None
      // ⇒ 恒拒 "No agent definition available"（先于一切副作用）——确定性拒答件，
      // caps 仍按能力面诚实声明
      caps = Set(Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Exec, Cap.Net("*")),
      description = "Spawn a self-clone sub-task worker (caller identity required) — bridged SubTask tool."
    ),
    Bridged(
      irName = "dev:tool:memorynote",
      toolName = "MemoryNote",
      // MemoryWrite（语义能力）+ FsWrite（物理通道：MemoryQueue 落 <dataRoot>/memory/
      // queue.jsonl）双声明
      caps = Set(Cap.MemoryWrite, Cap.FsWrite("*")),
      description = "Append/edit a memory note (persisted to the memory queue) — bridged MemoryNote tool."
    ),
    Bridged(
      irName = "dev:tool:schedule",
      toolName = "Schedule",
      // 侦察修正补 FsWrite：create/cancel 落 <dataRoot>/scheduled/<sessionId>.json 原子写
      caps = Set(Cap.FsWrite("*")),
      description = "Create/list/cancel scheduled tasks for the session — bridged builtin Schedule tool."
    ),
    Bridged(
      irName = "dev:tool:teamtaskcreate",
      toolName = "TeamTaskCreate",
      // 存储根 = <dataRoot>/tasks/teams/<name>/ 真实落盘 ⇒ FsWrite（侦察修正补入）。
      // 执行面：模板 teamName=None ⇒ 恒拒 "only available to team agents"（先于 store）
      caps = Set(Cap.FsWrite("*")),
      description = "Create a task on the caller's team board (team agents only) — bridged TeamTaskCreate tool."
    ),
    Bridged(
      irName = "dev:tool:teamtaskupdate",
      toolName = "TeamTaskUpdate",
      // 同 TeamTaskCreate：模板 teamName=None ⇒ 恒拒（确定性拒答件），caps 按写面声明
      caps = Set(Cap.FsWrite("*")),
      description = "Update a task on the caller's team board (team agents only) — bridged TeamTaskUpdate tool."
    ),
    Bridged(
      irName = "dev:tool:teamtasklist",
      toolName = "TeamTaskList",
      // 只读腿真实可用：taskStore 槽 = Some(FileTaskStore)（生产 ToolContext 同源），
      // team 参数显式传入即绕开 teamName ⇒ Nebula read-only oversight 既有语义
      caps = Set(Cap.FsRead("*")),
      description = "List a team's tasks (explicit team name, read-only) — bridged TeamTaskList tool."
    ),
    Bridged(
      irName = "dev:tool:load",
      toolName = "Load",
      // flow 腿纯读校验真实可用；team 腿 FlowTreeRegistry 需 agentActorRef（模板 None
      // ⇒ 显式错误收敛 exit 1）。FsWrite 保留证据：mount 成功腿 persistTeams 落盘
      caps = Set(Cap.FsRead("*"), Cap.FsWrite("*")),
      description = "Validate (and mount) a team or flow definition — bridged builtin Load tool."
    ),
    Bridged(
      irName = "dev:tool:agentcontrol",
      toolName = "AgentControl",
      // 无文件/网络/spawn 声明面：list/status 读 agentRegistry 真实可用；cancel 杀 OS
      // 进程树无词表可表达（openQuestions 留痕），不假装
      caps = Set.empty,
      description = "List/status/cancel running agents (registry read + guarded cancel) — bridged AgentControl tool."
    )
  )

  /** 全批共用管道契约：不吃 stdin、产出 text（J6：适配器只产 `StreamValue.Text`）。 */
  val io: CommandIo = CommandIo(stdin = None, stdout = StreamKind.Text)

  val pathArgs: Set[String] = Set.empty

  /**
   * 三十件统一由桥装配的字段（specs 的表驱动断言面，见 `IrToolBridge.toCommandDef`）。
   *
   * P1-3（audiences 翻转）：{Human} → {Human, Llm}——不翻则 `checkAudience`（两处
   * 检查点，C21/C25）把每个 llm ingress 对 `dev:tool:*` 的调用拒掉，整个改道死胎。
   * 翻转激活 §7.5 注册期校验（Llm∈audiences ⇒ llmName 必填且全局唯一，
   * `Registry.validateLlmName`）——桥接 def 已带 `llmName=Some(toolName)`。模型工具表
   * 从 TOOL_MAP 喂给（桥只读）⇒ 表逐字节不动。本翻转是 P1-3 的**代码常量**，不受
   * `ir.llmIngress` 开关翻转（D30 约束的是开关不改注册面，不是描述符不能演进）；
   * ws 直连 ir 帧的 llm 租户与 human 同信任边界（gateway 仅 token 级 auth）——
   * 是否显式拒待作者裁定（openQuestions 留痕，默认不拒）。
   */
  val audiences: Set[Audience] = Set(Audience.Human, Audience.Llm)
  val trust: Trust = Trust.Builtin

  /**
   * 策略面（§8.1 SHOULD 的落地）：`dev:tool:*` 一律 `Ask`——三十件全为 `prefix="*"` 的
   * VFS 外不透明访问，包含性判定不可做就不假装能判。规则按名前缀只罩 `dev:tool:*`
   * ⇒ `dev:fs:*` 的缺省行为（纯读放行）不变。`auto-all` 档把 `Ask` 折 `Allow` 是既有
   * 档位语义（`ir/Policy.scala` composeSafety）；即便摘掉本规则，Glob/Grep/NodeEdit 的
   * `Exec` 与全部写件也使缺省走 `Ask`（缺省分支），词表与兜底双保险。**显式 Allow
   * 规则（如 `dev:tool:write` 精确名）是唯一真写放行通道**——批 B spec 的正负两腿。
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
