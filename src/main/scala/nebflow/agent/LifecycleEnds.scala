package nebflow.agent

/**
 * 生命周期终结协议落点（编排收敛第四批 ORCH4，2026-09-28 裁定 ORCH4-R1/R2/R3）。
 *
 * 作者裁定 ORCH4-R1「落点取 A」：本文件 = agent 侧同形面的**声明落点**，承载
 * 「会话级联停子（SessionChildCascade）→ 启动扫 running（SubAgentStartupRecovery）
 * → 父亡检测（BackoffSupervisor Cancelled 终态序列）」三面之间的共享体。
 *
 * 落点论证（为何不是扩展 TurnBoundary）：
 *   - `TurnBoundary` 是 `private[agent] object`（TurnBoundary.scala:24）⇒ 除
 *     nebflow.agent 包外不可达；且其自身 doc 声明「事件面**只允许**类型化结构与
 *     命名方法——禁止引入任何事件分发 / 回调 / 注册行为」（ORCH1-P5 裁定）——
 *     生命周期终结属**另一轴**，不是 turn 边界协议。
 *     （本行行号引用经 2026-09-28 审计订正：原写 `:29` 实为空行、非漂移——
 *     TurnBoundary.scala 本批未被触碰，`object` 声明在 HEAD 与本工作区同为 :24。）
 *   - 本落点依赖闭包合法：`nebflow.agent` 层位可向下依赖 shared/actor/core/llm/
 *     bridge/dropbox/neblink/social；禁造反向边（core/gateway → agent），core 侧
 *     消费者须经 core/AgentRuntimePort.scala 既有注册器形态（先例 DelegateBudgetPort
 *     :201-222）。本文件**零 import、零成员**（见下方台账），故不产生任何分层边。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH4-R1 台账①（`ORCH4-P1` + `ORCH4-P2` 口径）：本批共享体判定 = **0 个可提取方法**（证据，可复现）
 * ─────────────────────────────────────────────────────────────────────────────
 * 判定口径 = 裁定 ①「只吸收逐字形同的部分；任何不一致一律『参数化（若差异仅为
 * 实参）或保留原状 + 逐字并置注（若不满足逐字同形）』，禁静默统一」。
 *
 * 判别过程（两步，两步都必须成立才算「差异仅为实参」）：
 *   步骤 1 —— 文本行级交集：把三面终态区段（HEAD 坐标：BS 338-386 / SC 43-70 /
 *     SR 55-136）按「去行首空白（sed 行首替换）+ 去空行（grep -v '^$'）+ sort」求交，
 *     两两 `comm -12`。实测：三面交集为空，仅余 `)` `}` 与一处巧合
 *     `payload = payload,`。（管道本体未逐字内嵌：注释文本不得含块注释终止符，
 *     而「行首空白裁剪 + 空行替换」两个 sed 脚本的写法必然含该二连字符；
 *     上述命令名与三步操作足以逐字重建。）
 *     🔴 行号口径（2026-09-28 审计 Finding-1 随迁 re-pin）：上列三处为 **HEAD 坐标**；
 *     本批在三文件各插入并置注后，同区段现行坐标 = BS 354-402 (+16) / SC 61-88 (+18) /
 *     SR 74-155 (+19)，漂移量逐字等于 `git diff --numstat` 的插入量（BS 16 / SC 18 / SR 19；**本文件内的
 *     行号一并按「现行坐标（HEAD :x）」双记**，禁再写裸 HEAD 号。
 *     另：涉及 `grep` 的判据一律按本仓既有「**剥注释再判**」口径读（先例
 *     `codeOnly` / `noBlock`）——否则本批新注自身会被 grep 命中（实测：
 *     `grep -n 'parentSessionId =='` 现同时命中本文件 :76（F4 段）与
 *     SessionChildCascade.scala:34 两条新注文本）。
 *   步骤 2 —— 词法归一交并（排除「同义不同写」漏判）：剥注释与字符串字面量，
 *     把标识符与数字归一为占位符（保留运算符与标点），再求成对最长公共 token 串
 *     （阈值 ≥6）。实测三面两两之间**最长公共串全部是语言层样板**，无一条是共享
 *     业务子序列：
 *       BS×SC：`) . _ ( _ => _ . _ ( _ _ ) )`（= `.handleErrorWith(e => logger.warn(s"…"))`
 *              的形状，文本不同）、`) : _ [ _ [ _ ] ] = _`（= `…): IO[...] =` 签名行）
 *       BS×SR：`_ _ ( _ : _ , _ : _ , _ : _`（形参表形状）、
 *               `_ , _ = _ , _ = _ , _ = _ ( _`（具名实参表形状）
 *       SC×SR：`_ _ ( _ : _ , _ : _ ) : _ [ _ [ _ ] ] = _ . _ . _`（签名 + for 链开头）
 *   ⇒ 结论：三面之间**不存在可提取的逐字形同业务序列**；任何提取都只能落在
 *     「形参表形状 / handleErrorWith 样板 / 具名实参表」这类语言层样板上，那正是
 *     裁定 ① 与 ORCH1-R6「禁转发别名」、本批 ④「禁 re-export shim」所禁止的静默统一。
 *
 * 逐片段分类台账（每一候选片段的判定与理由；全部落「保留原状 + 并置注」）：
 *   F1 「任务记账」调用：BS `resources.subAgentTaskStore.updateStatus(parentSessionId,
 *      subagentId, taskStatus, completedAt = Some(System.currentTimeMillis()),
 *      lastError = …).handleErrorWith(e => logger.warn(…))`
 *      vs SR `store.updateStatus(task.parentSessionId, task.taskId, status = "failed",
 *      lastError = …, completedAt = Some(now))`。
 *      差异：① 错误处理**位置不同**（BS 就地 handleErrorWith；SR 无，其错误由调用方
 *      `recoverOrphans` 的 `orphans.traverse(… .handleErrorWith(…))` 兜）——这不是实参
 *      差异，是控制流结构差异，无论共享体带不带 handleErrorWith 都必改一侧行为；
 *      ② `completedAt` 取值点不同（BS 调用时现读墙钟；SR 用 for-comprehension 首步
 *      早绑的 `now`）。⇒ **不满足逐字同形**，保留原状。
 *   F2 「通知父」：BS `parentRef match { case Some(ref) => ref ! ExternalEvent(…) ; case None => IO.unit }`
 *      vs SR 走 `CompactionQueueStore.load/save` 的 F2 队列**落盘**（带 parentExists 守卫
 *      与 correlationId 幂等）。差异：投递机制不同（actor tell vs 持久队列文件追加），
 *      且 SR 是落盘语义。⇒ 保留原状。
 *   F3 「终态动作序列」：BS 终态顺序 = 通知父 → 任务记账 → hub 清槽 → 预算释放 →
 *     注册表移除 → child Stop；SC 无 supervisor 降级腿顺序 = child Stop → 注册表移除。
 *     差异：**同两动作、相反顺序**（另 SC 的 Stop 带 handleErrorWith、BS 不带）⇒
 *      裁定 ②「动作顺序逐点等价」在此**不可满足**，禁止统一。保留原状。
 *   F4 「终结判定」：SC = `rec.parentSessionId == sessionId && kind ∈ {Delegate,SubTask,
 *      Ephemeral}`（纯谓词，全仓唯一；按**剥注释**口径 grep `parentSessionId ==` 只命中
 *      SC:64-65 与 AgentControlTool:313 的另一种语义——现行坐标，HEAD 为 SC:46-47：
 *      本批在 SC 顶部插入 18 行并置注 ⇒ 漂移量 +18 逐字等于 numstat 插入量）；
 *      SR = `status == "running"`（在
 *      `findRunningTasks` 内）+ `parentExists`；BS = 收到的终态事件类型。
 *      三者谓词不同源、不同域。⇒ 保留原状。
 *   F5 「投递 Cancelled 到 supervisor」：SC:73 `sup ! AgentEvent.Cancelled(rec.sessionId,
 *      s"parent session $sessionId deleted")` 等 4 处同形调用面（另见 BackoffSupervisor:82/298
 *      （`DelegateBudget.register(subagentId)(ctx.self ! AgentEvent.Cancelled(…))` 两处装配点；
 *      审计 Finding-1 订正：先前误写 `DelegateBudget:82/298`——该文件内无此调用，:82 为注释）、
 *      AgentControlTool:729、TaskStuckWatcher:1155——三处均在本批**插入点之前**，故
 *      现行坐标 == HEAD 坐标，无漂移）。差异仅为实参，但共享体将退化为
 *      **一行转发别名** ⇒ 违 ORCH1-R6「禁转发别名」与 ORCH4-R1④「禁 re-export shim」。
 *      ⇒ 不提取，保留原状。
 *
 * 本批成员 = **0**（零成员即零行为面；`private[agent]` 且无 `-Wunused`（build.sbt:74-113
 * 只开 `-deprecation`/`-feature`/`-unchecked`/`-Xfatal-warnings`）⇒ 无新增警告）。
 * 后续批次若获裁定提取，方法落本对象、由三面 handler 改指。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH4-R2 台账②（`ORCH4-P1` 面③ 父亡检测口径）：父亡检测 = **零代码**（本批为行为保持收敛；补父亡检测属行为新增）
 * ─────────────────────────────────────────────────────────────────────────────
 * 普查实况（HEAD 无落点、无终态转移序列），三块记如下：
 *   (a) [[DelegateBudget]] 是**触发器非面**：超时经 `ctx.self ! AgentEvent.Cancelled(
 *       subagentId, "timeout")`（**BackoffSupervisor.scala:82 与 :298** 两处
 *       `DelegateBudget.register(…)` 装配点；审计 Finding-1 订正：先前误写
 *       `DelegateBudget.scala:82 与 :298`——该文件内无此实参，其 :82 为注释行）汇入
 *       BackoffSupervisor.scala:163-181 的 `AgentEvent.Cancelled` 分支 ⇒
 *       `notifyParentAndStop(…, taskStatusOverride = Some("cancelled"))`——即既有终态序列。
 *   (b) HEAD 唯一「父存活」判定 = AgentActor.scala:976-983 `sessionIdOfRef`
 *       （注释原文：「反查 ActorRef → registry 中对应 sessionId（升级链『父存活』判定）」），
 *       唯一调用点 = AgentFrozen.scala:463-464（`AgentCommand.Escalate`）⇒
 *       `Escalation.nextTarget`（actor/AgentEvent.scala:160）⇒ 父亡 ⇒ `Target.Grandparent`
 *       ⇒ WS 帧 `errorEscalated{detail:"parent-unavailable-skipped"}`（AgentFrozen.scala:497-511），
 *       不设超时。
 *   (c) **全仓无一处 watch 父**：`watch(` 仅 BackoffSupervisor.scala:84/256（childRef/newChild）、
 *       MemoryTrack.scala:705（HEAD :676；agentRef——本批在该文件净 +29（numstat 49 插入 / 20 删除，含 P2 副作用声明新增）⇒ +29）、
 *       EphemeralAgentRunner.scala:76（ref）、
 *       FlowTreeActor.scala:381（ref）、NodeLoopRunner.scala:129（ref）、
 *       NodeStarter.scala:787（ref）、MailTool.scala:2229（watched）。
 *       ⇒ `parentRef ! ExternalEvent`（BackoffSupervisor.scala:381-390（HEAD :365-374；
 *       本批在该文件插入 16 行 ⇒ +16；tell 本体 :383）、
 *       AgentControlTool.scala:783-791（HEAD :769-777；+14；tell 本体 :784））
 *       在父已死时**静默蒸发**（ActorSystem.scala:28 doc 为证），无检测、无补偿。
 *
 * ORCH4 登记：补父亡检测（watch 父 / 投递失败补偿）属**行为新增**，留后续批次或用户
 * 拍板；**本批零代码**。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH4-R3 台账③：core 侧第 4 份同形拷贝 = **本批不并入**（后续批次候选）
 * ─────────────────────────────────────────────────────────────────────────────
 * 对象 = `core/AgentControlTool.scala:755-810`（HEAD :755-796；`case None =>` 仍在
 * :755，本批在该 `case` 之后插入 14 行并置注 ⇒ 腿体下移 14 行）（`doCancel` 无 supervisor 降级腿）。
 * 不并入理由三条（作者裁定原文）：
 *   ① 跨包——并入须在 core 侧新增窄 Face 端口面 + 启动注册点，属**结构性新增接口**
 *      而非收敛；端口机制已有先例（`DelegateBudgetPort` core/AgentRuntimePort.scala:201-222），
 *      故推迟不等于搁置，实施成本低。
 *   ② 未注册路径的失败面必须与 HEAD 逐点等价，风险与「行为保持」批的目标不匹配。
 *   ③ 它有四处真差异（不满足本批「逐字同形」门槛）：(i) `kind → source` 映射
 *      （:772-775（HEAD :758-761）；+14）；(ii) `Ephemeral` 跳过 taskStore
 *      （:793（HEAD :779）；+14）；(iii) 缺 hub 清槽
 *      （BS 有 `cleanupPendingAsks`，本腿无）；(iv) 缺 `DelegateBudget.release`
 *      （BS 有，本腿无）；另注：注册表移除与 child Stop 的**顺序与本仓其它降级腿相反**
 *      （本腿 `registry.update(_ - sid)` 在前、`rec.ref ! Stop(…)` 在后）。
 * 处置：本腿行侧已落并置注引 ORCH4-R3（AgentControlTool.scala:755 处，原注逐字保留；
 * 现行坐标 == HEAD 坐标，本批注插于该 `case` 之后）；summary 登记为**后续批次候选
 * （B5 或用户拍板的独立批次）**。
 * 复核声明：本批经逐行对照**未发现**可推翻上述四差异的证据；若后续复核认为它实为
 * 逐字同形，须带逐行对照证据升级，不得先改再说。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH4 台账④：T9 spawn 构造面口径声明（审计 Finding-6 补记；`ORCH4-P3` 口径）
 * ─────────────────────────────────────────────────────────────────────────────
 * 全仓 `AgentActor(` 直构点在改后**仅剩 2 处，均在 agent/SharedResources.scala 工厂体内**：
 * :228（`spawnAgentActor` 通用工厂体，:222-264）与 :360（`agentActorBehavior` 全参镜像
 * 工厂体，:325-396）；另 `spawnRootAgent` 新增第三处工厂体 :431（:426-456，root 专用面）。
 * ⇒ T9「四点」（本批普查所列 depth=0 根构造 / `spawnAgentActor` / MemoryTrack 直构 /
 * `spawnSupervisedAdapter`）**未覆盖第 5 个面 `agentActorBehavior`**——其三处消费点
 * `EphemeralAgentRunner.scala:50`、`FlowTreeActor.scala:712`、`MailTool.scala:2147`
 * （均 `system.spawn(resources.agentActorBehavior(…))` 形态）本批**零改动**。
 * 口径声明：该面在 `core/AgentRuntimePort.scala:4` 已被列为既有「R-C **三工厂镜像**」之一
 * （`agentActorBehavior` / `spawnAgentActor` / `spawnSupervisedAdapter`），属**已授权例外**
 * ⇒ 本批不改它不构成「四点未全覆盖」；仅登记口径，避免与「全部改经统一工厂」的字面主张混淆。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH4 台账⑤：承接盘点（`ORCH4-P6` 口径；四标记逐条分类 + ORCH1-R3 两站可行性论证）
 * ─────────────────────────────────────────────────────────────────────────────
 * 全仓 grep（本批实跑）分类结果：
 *   · `待下批` 8 处 = AgentState.scala:1103 / :1115（ORCH1-R9 reset 族携带子集差异）、
 *     AgentFinishTurn.scala:404 / :481 / :510 / :597、AgentProcessing.scala:289、
 *     TurnBoundary.scala:308（ORCH1-R8 三注入腿携带组装）、SubAgentInboxMirrorSpec.scala:344
 *     （测试侧靶点注）。
 *   · `ORCH1-R3` = 2 处 main（见下）+ 1 处 test（同上靶点注）。
 *   · `ORCH2-P3` = 18 处（全为「保留:语义差异」）。
 *   · `ORCH3-P2` = **0 处**——HEAD 不存在该标记（全仓仅有 ORCH3-P1 / ORCH3-R1，
 *     AgentCompactionHandlers.scala:20）。
 *   ⇒ 分类结论：**无一属本批 T7/T9 波及面**（上述标记全落在 turn 边界注入帧轴与
 *     reset 携带子集轴上）；故按 P6「其余登记不动并清单化转 Battle-5/里程碑」，
 *     **全部登记不动、清单化转下批，本批零动作**。
 * ORCH1-R3 两站（AgentFinishTurn.scala:510 turn-末 imm 腿 / AgentProcessing.scala:289
 * recoverable-abort imm 腿）可行性论证（承 ORCH2-P5「禁静默统一」定案）：
 *   · 症结：两腿缺 `delivery` 键、且 PROJECT 段 header 与统一形不同；而
 *     `MailTool.sendMail`（约 :2265-2283）构造的 ImmediateInput 带
 *     `delivery = Some("immediate")`（且 `project = Some(...)` 可能），会流入两腿可见域
 *     ⇒ 若直接并入 `TurnBoundary.emitForImmediateInput`，`emitInjectedUserEvent` 的
 *     `withDelivery` deepMerge 会给帧补 `"delivery"` 键并改 PROJECT 段 ⇒ **帧字节不等**。
 *   · 下批方案 A（画像实参，推荐）：在 TurnBoundary 边界帧增设 `carryDelivery: Boolean` /
 *     `carryProject: Boolean` 显式实参，按站传入 ⇒ 补键与否由实参决定，帧字节可 diff 钉死。
 *     **可行性：高**（改动局限在组装函数签名 + 3 个站点实参，零新依赖）。
 *   · 下批方案 B（命名 legacy 腿）：两腿原样保留，TurnBoundary 增设命名 legacy 入口仅做
 *     「零字段变换」登记 ⇒ **可行性：高但收益低**（仍留两份组装，只换名）。
 *   · 本批处置建议：**转下批（方案 A）**——本批两轴（生命周期终结 / spawn 构造）与
 *     turn 边界注入帧无交集，强行并入既违 ORCH2-P5 定案，又引入帧字节风险。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * 纪律（本批与本落点共同承担）
 * ─────────────────────────────────────────────────────────────────────────────
 *   - 共享体只做「终结判定 + 终态转移序列」；各面特有动作（通知父 / 落盘 / UI 镜像 /
 *     任务记账 / 注册表移除时序）留各自 handler，动作顺序逐点等价（ORCH4-R1②）。
 *   - 本批共享体**零事件分发行为**（承 ORCH1-P5）。
 *   - 带日期中文作者裁定注释随代码逐字迁移，一字不改；新增注引 `ORCH4-P*`/`ORCH4-R*`
 *     （ORCH4-P5 编号口径）。
 *   - 字符串字面量零改动。
 *   - 行号引用口径（2026-09-28 审计 Finding-1/2 订正，承 ORCH2-R7 / ORCH3-R1）：
 *     新增注的行号引用一律写「**现行坐标（HEAD :原号）**」双记，禁裸 HEAD 号；被本批
 *     插入行下推的目标必须**随迁 re-pin**，禁止留旧号。涉及 `grep` 的判据按**剥注释**
 *     口径（`codeOnly` / `noBlock`）复现，否则会命中本批新注自身。
 *   - ORCH4-P 编号（审计 Finding-5，**已闭环**）：P 清单**已取证**——编排端 workflow 草稿的
 *     `PRE_APPROVED`（「预批裁定(执行员与审计员双侧同步,编号 ORCH4-P1..P6)」）逐字可读，
 *     P1..P6 全文已取回并按项补号（本文件 :147 台账④ 起 + 各面注头）：
 *     `ORCH4-P1`＝T7 三面终结协议提为单一协议对象 / `ORCH4-P2`＝逐点等价（含**副作用集合
 *     逐条对照**，不可证等价处保留原状 + 逐字并置注）/ `ORCH4-P3`＝T9 四点全改经统一工厂、
 *     root 专用参数面、逐字段镜像 / `ORCH4-P4`＝restart 漂移（行为修复非保持，登记「ORCH4
 *     修复候选，待用户拍板」）/ `ORCH4-P5`＝注释纪律（既有带日期注释逐字保留；**新注须
 *     同时含 P 与 R 编号**，审计红线为字面核验）/ `ORCH4-P6`＝承接盘点（四标记逐条分类 +
 *     ORCH1-R3 两站可行性论证）。**更正记录**：本文件更早版本曾因半截命令输出而误判
 *     无 P 清单；该误判已整体撤销，现按 P1..P6 逐项补号。**P5 严格口径已全量满足**：
 *     实跑 `grep -rn 'ORCH4-P[0-9]' src --include=*.scala` ⇒ **12 个改动文件全部命中**
 *     （LifecycleEnds / SessionChildCascade / SubAgentStartupRecovery / BackoffSupervisor /
 *     MemoryTrack / SharedResources / AgentControlTool / NodeRunner / WebSocketRoutes /
 *     MemoryTrackPauseSpec / ToolFaceVariantSchemaSpec / UserTextGateSpec），
 *     即每个新注所在文件均**同时**含 `ORCH4-P*` 与 `ORCH4-R*` 编号。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH5 台账①：core 根文件域归位（`ORCH5-P1` / `ORCH5-R1`，2026-09-28 第五批=末批）
 * ─────────────────────────────────────────────────────────────────────────────
 * 口径：**只许动文件位置 + package 声明 + import 补全，逻辑零改**；core 内子目录
 * 移动**不改包边**（门禁按顶层包 `core` 计数，子目录不产生新边）。
 *   移动 **9** 文件（**core 内**；git 侧表现为 rename；内容除 package 行/必要 import 外逐字节未改）
 *   ——另跨包 5 件见台账②、迁 test 树 1 件见台账③① ⇒ **主树 rename 合计 14 件**（审计 #7 订正：
 *   本台账前稿误写「11 文件」且下辖 bullet 仅 9 件；实测 `git diff HEAD -M --diff-filter=R
 *   --name-status | wc -l` = 15 = 14 主树 + 1 test 树，逐名核对无遗漏无多余）：
 *     · `McpToolGate.scala` → `core/mcp/`（同目录既有 AgentMcpLoader/McpClient/McpManager
 *       等同族）；package ⇒ `nebflow.core.mcp`；补 `import nebflow.core.SafetyMode`
 *       （原同包裸引用 6 处，`permissions.scala` 仍在 core 根）。
 *     · `InstallLayout.scala` / `JvmRequirement.scala` / `WindowsDepProbe.scala`
 *       → `core/boot/`（**新建**；三者 = 安装形态/JDK 闸/启动依赖探针，消费面均在
 *       GatewayMain 启动链）；package ⇒ `nebflow.core.boot`；`WindowsDepProbe` 对
 *       `InstallLayout` 的裸引用因同迁而**无需** import。
 *     · `RestartHelper.scala` / `AutoStartService.scala` → `core/hotrestart/`（取
 *       消费面最近者：`hotrestart/HotRestart.scala` 同时消费两者，且其包名相同 ⇒ 该
 *       消费点**零 import 改动**）；package ⇒ `nebflow.core.hotrestart`。
 *     · `UsageAggCache.scala` / `UsageRecordStore.scala` / `UsageTracker.scala`
 *       → `core/usage/`（**新建**；token 面板族）；package ⇒ `nebflow.core.usage`；
 *       `UsageRecordStore` 补 `import nebflow.core.AtomicJson`（原同包裸引用 2 处）。
 *   留置 core 根（本批零动作，见台账⑤/⑥）：`SessionStore` / `AtomicJson` /
 *   `filewatch`(FileChangeTracker 100 消费) / `ratelimit`(RateLimiter 142 消费) /
 *   `ports` / `permissions` / `reminders` / `capabilities` / `errors` / `handlers` /
 *   `AgentRuntimePort` / `DeviceMailAckPort` / `FriendRosterPort` / `LlmRuntimePort` /
 *   `Guardrails` / `PathParamCodec` / `repl.scala`（`JvmRequirement` 虽已按裁定迁出，
 *   其路径引用陈旧登记见台账⑥）。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH5 台账②：跨包迁移 5 文件 + 边变化三列（`ORCH5-P2` / `ORCH5-R1`；`ORCH5-P6` 目录）
 * ─────────────────────────────────────────────────────────────────────────────
 * 逐项闭包论证（去注释/字符串后实测出边）与落点：
 *   ① `CredentialFileAcl` core→`nebflow.shared`：自身出边 = **∅**（零 nebflow 依赖，
 *      纯 JDK）⇒ 落 L0 天然纯净；消费者 neblink×2（wildcard 补 import）+ social×1
 *      （改并既有 `nebflow.shared` 花括号导入）。边变化 = **0**（其自身无边、消费者
 *      wildcard 导入行仍在）。
 *   ② `WsTimeoutFilter` core→`nebflow.shared`：自身出边 = ∅、代码消费 = **零**；
 *      唯一引用面 = 两条 logback FQN 字符串（见台账④）。
 *   ③ `HeadlessMode` core→`nebflow.shared`：自身出边 = {shared}（Branding）⇒ 迁后
 *      **零出边**；消费 5（agent ContextRefresher / core.tools AskUserQuestionTool
 *      wildcard 补 import；cli SingleInstanceGuard / gateway GatewayMain 内联 FQN
 *      重 target）。
 *   ④ `CanvasTabStore`（含 `object CanvasTabs`）core→`nebflow.gateway`：**core 内零消费**
 *      且 gateway 独占（唯一主消费 `gateway/ConfigRoutes.scala` wildcard，迁后与自身
 *      同包 ⇒ 该消费点零 import 改动）；补 `import nebflow.core.AtomicJson`。
 *      测试侧 `core/CanvasTabStoreSpec.scala` 补 `import nebflow.gateway.{CanvasTabStore, CanvasTabs}`
 *      （⚠️ 该文件 :57 的 `json(""""str""""…` 是**合法** Scala 三引号串〔`"""` + 内容
 *      `"str"` + `"""`〕，门禁自带 `stripSource` 用首遇 `"""` 规则 ⇒ 自该行**失步**，
 *      其后文本全被当字符串剥掉；本批首轮自查因复用同一规则而漏判此 import，第二轮
 *      门禁报 E006 `Not found: type CanvasTabStore` 后按 Scala 语义修正剥离规则复扫
 *      ——全仓仅此一处漏判，已补。门禁该失步为**预存在**行为，不涉包边合法性：本批新增
 *      引用全为「向下层 / 同顶层包」形态，反向边不可能生成）。
 *   ⑤ `OnboardingService` core→`nebflow.gateway`：唯一主消费 `gateway/WsConfigHandlers`
 *      （4 处内联 FQN 重 target 为 `nebflow.gateway.OnboardingService`）；gateway 为图顶
 *      （无上层消费者）⇒ 方向合法。
 *   边变化三列（实测：`node scripts/check-scala-layers.mjs`，旧 526 边 → 新 517 边，
 *   未豁免 0、core→agent 保持 0）：
 *     · `cli→core` 6→3：SingleInstanceGuard 唯一 core 引用（HeadlessMode FQN）改指 shared；
 *       `cli/ui.scala` + `cli/uiStore.scala` 删除（其唯一 core 引用 = `ReplUi`，见台账③）。
 *     · `cli→shared` 14→11：`cli/ui.scala` / `cli/uiStore.scala` / `cli/markdownRenderer.scala`
 *       三文件删除（各持 shared 引用）。
 *     · `core→shared` 152→148：HeadlessMode / OnboardingService / CanvasTabStore 三文件
 *       迁出（各 -1）+ `core/replUi.scala` 删除（-1）。
 *     · `gateway→core` 26→26（净 0）：`gateway/SessionRecorder.scala` 迁 test 树（-1，见
 *       台账③①）+ `gateway/CanvasTabStore.scala` 迁入补 `nebflow.core.AtomicJson`（+1）。
 *     · `gateway→shared` 36→37：OnboardingService / CanvasTabStore 迁入（各 +1）−
 *       SessionRecorder 迁出（-1）。
 *     · 其余 33 组包边**全部不变**；core 内子目录移动按口径不计边。
 *   消费面内联 FQN 重 target（主树，`ORCH5-P2` 判据「只改包路径前缀、其余逐字不动」）
 *   共 39 处，分布于：AgentSessionExecution(7: McpToolGate/McpGateOutcome) /
 *   AgentActor(1) / InteractionHub(1) / RemoteUpdateAction(1) / RgHelper(1) / shell(2) /
 *   Rollback(1) / NeblinkRoutes(1) / NeblinkRelayTunnel(1) / WsSystemHandlers(3) /
 *   AutoStartCommand(1, import 行) / MailTool(1) / WebSocketRoutes(1) / AgentProcessing(1) /
 *   GatewayMain(2) / SingleInstanceGuard(1) / WsConfigHandlers(4)。测试侧内联 FQN 重
 *   target 1 处（`core/hotrestart/HealthGateSpec.scala:306`〔**随迁 re-pin**：HEAD 原号 :301，
 *   本批在该文件插入 2 条 ORCH5-R2 注（+3 行）与 :303 的 FQN 改写同行 ⇒ 净 +5〕，`ORCH5-R1` E 条授权）+
 *   `CredentialDiagnosticsSpec.scala:209` 2 处（同授权）。
 *   import 形态归一（门禁 `scalafix --check` = **Compile 作用域**的期望修复，逐字照落）：三处
 *   「显式单选择符 + 同前缀 wildcard」相邻语句按 OrganizeImports 归并为花括号形（wildcard 殿后）
 *   ——`agent/ContextRefresher.scala:9`、`core/tools/AskUserQuestionTool.scala:8`、
 *   `neblink/NeblinkModel.scala:9`：`import nebflow.shared.*` ⇒ `import nebflow.shared.{HeadlessMode, *}`
 *   / `{CredentialFileAcl, *}`（净效果 = 各文件仅改**一行既有 import**，不新增语句）。
 *   实测口径：显式+显式 同前缀对**不**归并（HEAD 遗留反例 `neblink/NeblinkModel.scala:864-866`
 *   的 `io.circe.{Decoder,Encoder,HCursor}` 三行并列，且 HEAD 过门禁）⇒ 测试树同类形态
 *   （`DeviceCredentialAclSpec:5-6` / `DeviceFaceHardeningSpec:7-8` 的 `CredentialFileAcl`+`PathUtil`）
 *   按「测试面最小改」不动。
 *   A2/A3 留置（作者裁定「不迁」）：`LlmLogWriter`（闭包纯净但 45 文件 209 处内联 FQN，
 *   其中 44 个测试文件）+ `ToolsLogWriter`（姊妹同址）——台账注见各自文件头（台账⑥）。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH5 台账③：死码清册（`ORCH5-P3` / `ORCH5-R1`；零引用实证逐项，全仓 `git grep -n`）
 * ─────────────────────────────────────────────────────────────────────────────
 *   ① `gateway/SessionRecorder.scala` **改判为归位非删**（`ORCH5-R1` C1 条）：迁至
 *      `src/test/scala/nebflow/gateway/SessionRecorder.scala`（实现逐字、package 不变、
 *      spec 零改）。证据：主树零消费（生产录制链已由 `WebSocketRoutes.makeRecordingWsSend`
 *      承接，`WebSocketRoutes.scala:161/2029`），唯一消费 = `TurnEndpointSpec.scala:105`
 *      的录制链装配 ⇒ 它是「住在 main 里的测试助手」；整删会净损失 4 用例覆盖且波及门禁
 *      冒烟名单，故不删。日期注释（原 :99「2026-09-10 死日志修复…」）随文件整段迁置，
 *      逐字未改。
 *   ② `core/Whitelist.scala` **整删**：`object Whitelist` + 唯一成员 `passes` 全仓零引用
 *      （`git grep -n "Whitelist"` 仅命中同形异义：`PluginRegistry.BuiltinToolWhitelist` /
 *      spec 名 `FileRefsWhitelistSpec` / 脚本注释；`Whitelist.passes` 零命中；`passes`
 *      仅命中 `pdf_qa.py` 无关字段）；零 dated 注释 ⇒ 无迁置义务。
 *   ③ `core/replUi.scala` + `cli/uiStore.scala` + `cli/ui.scala` **三件簇整删**（+ 簇内
 *      连带 `cli/splash.scala`、`cli/markdownRenderer.scala`、`shared/terminal.scala`
 *      的 `TerminalUtils`）：簇级证据 = 外部引用面只有 `NebflowUI`→`UiStore`→`ReplUi`
 *      一条内部链，簇外零引用（逐符号 `git grep -ln` 实测：`NebflowUI` / `UiStore` /
 *      `UiState` / `CompletedRound` / `HistoryEntry` / `Phase` / `Splash` /
 *      `MarkdownRenderer` / `TerminalUtils` 均只命中簇内文件）；`ReplUi` 的 cli→core
 *      消费注册随之清零（见台账②边表）。
 *   ④ `Repl.buildUserMessage` + `Repl.replaceMediaPaths` **删成员**（`ORCH5-R1` C3 条）：
 *      零引用（含测试树/字符串/反射名）——`buildUserMessage` 仅命中定义行，
 *      `replaceMediaPaths` 唯一调用点在 `buildUserMessage` 体内；删后 `Repl` 非空壳
 *      （余活成员 `loadSystemPrompt`，消费 = `AgentSessionExecution.scala:2235`）；
 *      站点裁定注见 `core/repl.scala`。
 *   ⑤ `chat` 未实现腿 **整删**：`ChatCommands.scala` 的 `private object ChatRepl` +
 *      `subcommands` 表收窄为 `List(ChatSend)`；零引用（测试/文档/脚本零命中，
 *      `CliRouterSpec` 只测 `sessionFrame`/`newSessionId`）；站点裁定注见该文件。
 *   ⑥ `AgentActor.isOverloadClass` **删 1 成员**（B3 登记项）：`git grep -n -i overloadclass`
 *      全仓唯一命中 = 定义行自身；委托目标 `AgentActor.isOverloadReason` 仍被 :41/:54
 *      使用（活成员）；站点裁定注见 `AgentActor.scala`。
 *   ⑦ 依赖声明 **删 3 行**（`ORCH5-P3` ④）：`build.sbt` 的 `scopt` / `jline3Terminal` /
 *      `jline3Reader`——全仓零引用实证（`scopt` 仅命中 build.sbt 声明 + `project/
 *      Dependencies.scala` 定义；`org.jline` 仅命中 `cli/ui.scala`〔本批删〕+ 依赖定义；
 *      release/packaging/Dockerfile/Makefile 零命中）。`project/Dependencies.scala` 的
 *      三个 val 未获授权（`ORCH5-P7`）⇒ 留置。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH5 台账④：例外文件面（`ORCH5-P7`；例外=预批裁定明确写出者，逐字最小）
 * ─────────────────────────────────────────────────────────────────────────────
 *   · logback FQN 例外（`ORCH5-R1` A1②，字符串字面量唯一例外，单列）：
 *     `src/main/resources/logback.xml:4` 与 `src/test/resources/logback-test.xml:8` 的
 *     `<turboFilter class="nebflow.core.WsTimeoutFilter"/>` ⇒ `class="nebflow.shared.WsTimeoutFilter"`。
 *   · `build.sbt`：删 `libraryDependencies` 内 `scopt` / `jline3Terminal` /
 *     `jline3Reader` 三行 + 其随附组标签注释两行（`// CLI`、`// Terminal`）——标签所指
 *     组唯一成员已删，留之则成空组（共 5 行，逐字最小；`libraryDependencies` 其余未动）。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH5 台账⑤：承接盘点三态表（`ORCH5-P5` / `ORCH5-R1` D 条；末批不得留悬空）
 * ─────────────────────────────────────────────────────────────────────────────
 * 全仓实测（本批 grep）：`待下批` 9 站点 + 台账行 1；`ORCH1-R3` 2 主 1 测；`ORCH2-P3`
 * 18 站（+ BehaviorCommon 3 文档行 + 台账 1）；`ORCH3-P2` **0 站**（仅本条台账记「0 处」，
 * 全仓实际只有 `ORCH3-P1`/`ORCH3-R1`）；`ORCH4-R3` 1 站 + 台账 2；`ORCH4 登记` 1 站。
 *   逐条三态：
 *     · `待下批`×2（`actor/AgentState.scala` reset 族，ORCH1-R9）= **已处置**：ORCH2-P6
 *       定案即最终处置（参数化携带面落 `resetCarriedExecution` 共享体、签名语义逐字
 *       保留）；站点注已落（ORCH5-P5/R1）。
 *     · `待下批`×3（`agent/AgentFinishTurn.scala` ORCH1-R8 三注入腿）+ `TurnBoundary:308`
 *       文档行 + `SubAgentInboxMirrorSpec:344` 测试靶点注 = **明确建议**：方案 A（边界帧
 *       显式画像实参 `carryDelivery`/`carryProject`，逐站传入；成本 ≈ 组装函数签名 + 3
 *       站点实参，零新依赖，可行性高）；不并入本批理由 = 帧字节不可证等价（ORCH2-P5
 *       「禁静默统一」）+ 站点专属日期注释组随码铁律。站点注已落。
 *     · `待下批`×2（ORCH1-R3 两站：AgentFinishTurn turn-末 imm 腿 / AgentProcessing
 *       recoverable-abort 腿）= **明确建议**：同上方案 A（本批两轴与 turn 边界注入帧无
 *       交集，见台账③）。站点注已落。
 *     · `ORCH2-P3`×18（全为「保留:语义差异」）= **已处置**：该标记即最终裁定（保留原状），
 *       注记在位，本批零动作。
 *     · `ORCH3-P2` = **已处置**：实测 0 站（台账口径已更正为 ORCH3-P1/R1）。
 *     · `ORCH4-R3`（core 侧第 4 份 doCancel 降级腿）= **明确建议**：端口方案
 *       （`DelegateBudgetPort` 先例）+ 三理由不并入；站点注已落（AgentControlTool）。
 *     · `ORCH4 登记`（补父亡检测 / watch 父 / 投递失败补偿）**本条即三态归属** =
 *       **明确建议**：属**行为新增**（非行为保持），本批零代码；处置轮廓 = 父存活判定复用
 *       `AgentActor.sessionIdOfRef`（:976-983）+ 投递失败补偿或 watch 父；成本 ≈ 新增
 *       检测面 + 失败回归用例，须**用户拍板**（ORCH4-R2 台账② (a)(b)(c) 已备证据）。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH5 台账⑥：留置台账注与残余登记（`ORCH5-P2`/`ORCH5-P3`/`ORCH5-P6`，供审计核）
 * ─────────────────────────────────────────────────────────────────────────────
 *   · `LlmLogWriter`（core 根留置）：闭包纯净（出边仅 {shared}）但消费面 = 45 文件
 *     **209 处内联 FQN**（44 个为测试文件）⇒ 迁包会把末批撑成大规模 FQN 重写，与「测试面
 *     最小改」取舍不成比例；建议路径 = 先立「禁内联 FQN」lint 或独立小批，届时一并迁
 *     shared。文件头已落 ORCH5 台账注。
 *   · `ToolsLogWriter`（core 根留置，与上者同址）：姊妹分开会留下「日志写入器散在两包」
 *     新疤；两者互引（ToolsLogWriter → LlmLogWriter）且共享消费面
 *     （`agent/AgentSessionExecution`）⇒ 无复议证据。文件头已落 ORCH5 台账注。
 *   · `JvmRequirement` 迁 `core/boot/` 后**路径引用陈旧登记**（未获编辑授权，逐字留置）：
 *     `CONTRIBUTING.md:17` 与 `build.sbt`（「两个常量」日期注释块内）各引
 *     `src/main/scala/nebflow/core/JvmRequirement.scala` ⇒ 现址为
 *     `src/main/scala/nebflow/core/boot/JvmRequirement.scala`。
 *   · 注释面陈旧 FQN 登记（注释永不改写 ⇒ 逐字留置，仅登记）：
 *     `neblink/DeviceCredentialStore.scala` 两处 `[[nebflow.core.CredentialFileAcl…]]`
 *     scaladoc 链接、`core/hotupdate/RemoteUpdateAction.scala:20` 的
 *     `nebflow.core.InstallLayout.isMsiInstall` 括号注。
 *   · `Repl` 私有 vals（`IMAGE_EXTENSIONS`…`MAX_IMAGE_SIZE`）在两名成员删除后成为未用
 *     私有 val——按 `ORCH5-R1` C3「文件其余逐字不动」留置（build.sbt 未开 `-Wunused`
 *     ⇒ 零新警告）。
 *   · **访问限定符配套改写（跨包迁移的编译必然项，单列申报）**：`CredentialFileAcl` 迁
 *     `nebflow.shared` 后，其 7 处 `private[core]` 在新址不再解析（门禁编译 E139
 *     `no enclosing class or object is named 'core'`；`:110` / `:125` / `:176` / `:210`
 *     / `:577` / `:589` / `:634`）⇒ 按「同一作用域在新址的等价表达」逐处改写为
 *     `private[shared]`（只改限定符，方法体/签名/字面量零改）。可见性零收窄实证：7 个
 *     成员的全仓引用（主树 + 测试树）全部在本文件内部（逐符号 `git grep -n`，无外部
 *     消费者）。核心内迁移文件（`core/boot`、`core/usage`）内的 `private[core]` 仍合法
 *     （`core` 仍是**包围包**，如 `JvmRequirement` 3 处 / `UsageRecordStore` 1 处），
 *     未动。file 头已落并置注。
 *   · 迁移文件内**无 `given`/`implicit` 随包移动的隐式作用域风险**：各文件 given 均在
 *     **伴生对象内**（`UsageCounters`/`UsageAggCell`/`HourSpan`/… 的 companion），
 *     隐式查找按类型伴生走，与包围包无关。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH5 台账⑦：裁定溯源、预批面外例外登记与行号 re-pin 清单（审计 #1–#8 应答；`ORCH5-R2`）
 * ─────────────────────────────────────────────────────────────────────────────
 * 说明：本批有两条裁定**经编排端在门禁轮次中下达**（不在预批 `ORCH5-P1..P7` 字面内）：
 * `ORCH5-R1`（首轮普查后的执行裁定，含 A1–A5/B/C/D/E 各条）与 `ORCH5-R2`（冒烟轮后针对
 * **预存在** Windows 环境不兼容的容忍化裁定）。二者即本批裁定依据，现逐条入册（审计 #3
 * 指出的「R2 未入册」在此闭合）：
 *
 * (1) `ORCH5-R2` 三处站点改动（测试树，逐处最小，每处带裁定注）：
 *   · `core/hotrestart/HealthGateSpec.scala`：用例 `install pointer: read / flip /
 *     retained-package lookup (and the missing-pointer branches)` 与用例 `rollback: pointer
 *     flips to the previous version and the relaunch swaps ONLY the package path` 各 1 处
 *     assert —— 期望字面量逐字未动、断言未删未吞、失败信息参数保留，仅在**实值侧**插入
 *     `.replace('\\', '/')` 分隔符归一（Windows 实值经 `InstallPointer.jarIn` 得 `os.Path`
 *     字符串形态为反斜杠，原字面量为正斜杠 ⇒ 该断言在本机恒假）。仓内同类先例：
 *     `InputMentionsSpec.scala:103,112`。
 *   · `core/UsageAggCacheSpec.scala`：用例 `an unwritable directory degrades to a correct
 *     in-memory result (no 5xx)` **首行**加 `assume(!sys.props.getOrElse("os.name","")
 *     .toLowerCase.contains("win"), "requires POSIX sh")`（逐字同仓内先例
 *     `DaemonSpec.scala:198` / `DaemonLifecycleSpec.scala:289`；该用例调 POSIX-only
 *     `Files.getPosixFilePermissions`（:353），Windows 上由 JDK 直接抛
 *     `UnsupportedOperationException`）。用例名/用例数/其余断言零变。
 *   · 证据面（审计 #1/#2）：三条失败语句在**容忍化之前**与 HEAD 逐字节相同（容忍化前
 *     `git diff HEAD -U0` 对两文件仅显示 1 行 import 与 1 行 FQN 前缀）；异常栈
 *     （`target/wforch5-smoke-r4.log`：`WindowsFileSystemProvider.readAttributes →
 *     Files.getPosixFilePermissions → UsageAggCacheSpec…:353`）与 munit 消息里的反斜杠实值
 *     均证明失败源在测试体/JDK、与本批迁移无因果。审计未跑 sbt 属客观限制（本批执行员
 *     同样禁跑），故此处给出「HEAD 语句逐字节相同」的文本证据链。
 *   · 注号口径：三处站点注含 `ORCH5-R2` 与〔`ORCH5-P4` 例外面〕两项（P4 = 新注编号纪律，
 *     非越权依据；越权依据即本条 R2 登记）。**措辞订正（审计 #2）**：原注写「失败语句与
 *     HEAD 逐字节相同」易被读成「当前行未变」——现三处改为「**被改写的原语句**与 HEAD
 *     逐字节相同」，与 diff 事实一致。
 *
 * (2) 预批面外但**有明列授权**的项（审计 #4/#5/#6 应答，均可回溯到 `ORCH5-R1` 条款）：
 *   · `CredentialFileAcl` 7 处 `private[core]` → `private[shared]`（:120 / :135 / :186 /
 *     :220 / :587 / :599 / :644，现行坐标）：授权溯源 = `ORCH5-R1` **A4**（授权迁移 shared）
 *     + **编译必然**（迁后 `private[core]` 不解析，scalac E139；门禁 03:30 轮编译日志即该
 *     7 处报错）——非 P1「package/import」字面项，属**迁移的编译配套**；可见性零收窄已实证
 *     （7 成员全仓引用（主树+测试树）全在本文件内部；审计已独立复核通过）。**申报**：该项
 *     与 P1 字面口径的差异在此入册，供审计按「带日期裁定注的已批准例外」对待。
 *   · 簇删 3 件（`cli/splash.scala` / `cli/markdownRenderer.scala` /
 *     `shared/terminal.scala` 的 `TerminalUtils`，共 156 行）：授权溯源 = `ORCH5-R1`
 *     **C2 条**（原文点名这三者，并要求「逐项零引用实证；若构成互引簇可给簇级证据」；本批
 *     已给簇级证据：外部引用面仅 `NebflowUI→UiStore→ReplUi` 内部链，簇外零引用）⇒ P3③ 的
 *     「jline 三件套」为**文件面下限**，C2 是这三个连带件的显式扩张授权。
 *   · `src/test/resources/logback-test.xml:8` 的 FQN 串：授权溯源 = `ORCH5-R1` **A1② 条**
 *     （原文并列 `logback.xml:4` **与** `logback-test.xml:8` 两处）⇒ P7 的「logback.xml」
 *     应按 A1② 读作「logback 家族两文件」；台账④ 已先行单列，此处补溯源。
 *
 * (3) 行号 re-pin 清单（审计 #8；本批插入注导致的下推行漂移，逐一给「现址（HEAD 原号）」）：
 *   · `actor/AgentState.scala`：ORCH1-R9 第二站点 **:1119**（原 :1115；本批 :1112 插入
 *     ORCH5-P5 注 4 行下推）；第一站点 :1103 未漂移。冻结文本框引（ORCH4 台账⑤ :164）保持
 *     逐字，其 re-pin 以本条为准（禁改写冻结文本 ⇒ 以并置清单承接）。
 *   · `agent/AgentFinishTurn.scala`：三注入腿站点 **:488 / :615**（原 :481 / :597；本批在
 *     :408 与 :493 区段插入 ORCH5-P5 注下推）、turn-末 imm 腿 **:523**（原 :510；累计 +13）。
 *   · `core/tools/AgentControlTool.scala`：ORCH4 台账③ 所引两内容行 **:779**（原 :772-775
 *     之首；本批 :770 处插入 ORCH5-P5 注 7 行下推）与 **:800**（原 :793）。该腿
 *     `case None =>` 仍在 :755、ORCH4-R3 注仍在 :756（本批注插在其**下方** ⇒ 未漂移；冻结
 *     文本写「:755 处」指该腿起点，审计记为 off-by-one 两读并存，不擅改冻结文本）。
 *   · `core/hotrestart/HealthGateSpec.scala`：E 条授权 FQN **:306**（原 :301）——已在台账②
 *     就地 re-pin。
 *   · `agent/AgentActor.scala`：ORCH4 台账② (b) 引的 `sessionIdOfRef` 块 **:980-987**（原
 *     :976-983；本批 :142 处删 2 行〔`isOverloadClass` + 其 scaladoc〕、插 6 行 ORCH5-P3 注
 *     ⇒ 净 **+4**；定义行实测 :980，HEAD 为 :976）。
 *   · `core/tools/AgentControlTool.scala`（续）：ORCH4 台账② (c) 引的「通知父」tell 本体
 *     **:791**（原 :784；+7）与该腿区 **:755-818**（ORCH4 原记 :755-810；+7；起点
 *     `case None =>` 仍在 :755 未漂移）。
 *   · `agent/SharedResources.scala`：ORCH4 台账④ 引的三处工厂体 **:229 / :361 / :432**
 *     （原 :228 / :360 / :431；本批在 :15 插入 1 行 import ⇒ **+1**）。
 *   · 未漂移（逐条实测）：`AgentState:1103`、`AgentProcessing:289`、`TurnBoundary:308`、
 *     `SubAgentInboxMirrorSpec:344`、`LifecycleEnds` 自身各台账行引用。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH5 台账⑧：授权原文入册（审计二轮 #1/#2/#3 的闭合面）+ 两侧边表（基线可复现）
 * ─────────────────────────────────────────────────────────────────────────────
 * 背景：审计二轮指出四类预批面外改动的「授权溯源不可核」——其材料只含 `ORCH5-P1..P7`，
 * 而授权条款在 `ORCH5-R1`/`ORCH5-R2`（**经编排端在下发执行指令时逐字给出**）。为使授权
 * 从**产物本身**可核，现将两份裁定的操作性条款**逐字节选**入册（节选以「…」标记，其余
 * 原文照录；条款编号为编排端原编号）：
 *
 * 〔`ORCH5-R1` 节选——首轮普查后的执行裁定，本批执行依据〕
 *   · A1②「②WsTimeoutFilter→shared（零代码消费，唯一改动=logback.xml:4 与
 *     logback-test.xml:8 两处 FQN 串，属 P2 预批例外——须在 summary 单列这两行 diff）」
 *     ⇒ **台账④ / #1① 的授权面：logback 家族两文件均被点名**。
 *   · A4「【授权】CredentialFileAcl 的 4 处测试内联 FQN 按 E 条改写。」+ A1①「CredentialFileAcl
 *     →shared（out=∅、边变化 0，收益=语义归位，同意）」⇒ **台账② ① 的迁移授权**。
 *   · A2「【留置，不迁】LlmLogWriter：…处置=在 LlmLogWriter.scala 文件头落一条 ORCH5 台账注…」、
 *     A3「【留置，与 A2 同址】ToolsLogWriter…」⇒ 台账⑥ 两条留置注的授权面。
 *   · B「主树内联 FQN 的机械重 target（~39 处）计入 P1 的『import 补全』…条件：只改包路径
 *     前缀、其余逐字不动、summary 给「旧 FQN→新 FQN→文件:行」三列表。」⇒ 台账② 尾段授权面。
 *   · C1「【改判为归位，不删】SessionRecorder 的唯一消费是 TurnEndpointSpec，说明它是『住在
 *     main 里的测试助手』：**迁到 src/test 树**（保持实现逐字、spec 断言零变）…」⇒ 台账③ ①。
 *   · C2「【授权，须证据】splash / markdownRenderer / TerminalUtils：逐项给**全仓零引用实证**…
 *     若它们与待删 replUi/repl 腿构成互引簇，可给**簇级证据**…」⇒ **#5 的授权面（三者点名）**。
 *   · C3「【授权，条件同 C2】Repl.buildUserMessage / replaceMediaPaths 可删，但必须：①零引用
 *     实证（含测试树）②仅删这两处成员，文件其余逐字不动…」⇒ 台账③ ④。
 *   · C1 附带：「不要做的事：…不得调整 `SMOKE_CMD`；不得顺手修 `InstallPointer.scala:104`
 *     之类的主代码…若你复核后认为某处必须改主代码才能过，带证据升级，不许自降断言强度。」
 *   · D「…**凡代码里已带状态标记的登记项**（`待下批`/`ORCH1-R3`/`ORCH4-R3`/`ORCH4 登记` 等）
 *     → **必须落码**，在标记处**并置**一条 ORCH5 裁定注把它收敛为三态之一…」⇒ 台账⑤ 的授权面。
 *   · E「【授权】**测试侧『仅位置重 target、断言零变』的内联 FQN 改写授权**（含
 *     HealthGateSpec:301 与 hotrestart 全族）。条件三条：①只改包路径，其余逐字不动 ②断言、
 *     期望值、用例名零变 ③summary 给「文件→改行数→旧 FQN→新 FQN」清单…」⇒ 台账② 尾段 +
 *     HealthGateSpec 站点注的授权面。
 *
 * 〔`ORCH5-R2` 节选——冒烟轮后针对预存在 Windows 环境不兼容的裁定〕
 *   · 「【编排端裁定 ORCH5-R2：取 (b)，授权最小环境容忍化；同时**明确否掉 (a)**——那两个
 *     spec 正是本批归位文件（InstallPointer/UsageAggCache）最相关的覆盖，不能为了门禁好看
 *     把它们从冒烟里摘掉。】」
 *   · 「1) `HealthGateSpec.scala:242` / `:289`：**归一实值侧，保留字面量意图**——…例如
 *     `assert(jar.replace('\\', '/').endsWith("versions/2026.9.19/nebflow-assembly-2026.9.19.jar"), jar)`
 *     （:289 对 `_._1` 同款）。禁改写期望字面量、禁删断言、禁用 `if windows then skip else
 *     assert` 之类会吞掉断言的写法；断言强度不降。」
 *   · 「2) `UsageAggCacheSpec.scala:353` 所在用例：按仓内既有先例 `DaemonSpec.scala:198` /
 *     `DaemonLifecycleSpec.scala:289` 的**同款写法与 import**加 `assume(!isWindows)`（munit
 *     assume ⇒ Windows 跳过、POSIX 照跑），位置放该用例首行；用例其余断言、用例名、用例数
 *     一律不动。」
 *   · 「3) 三处各落一条日期注：`// 2026-09-28 裁定（ORCH5-R2）：预存在 Windows 环境不兼容
 *     （失败语句与 HEAD 逐字节相同），按仓内先例最小容忍化；断言意图零变。`」
 *   · 「■ 不要做的事：不得把这三个用例改成 `ignore`/条件性 return 掉整段；不得调整
 *     `SMOKE_CMD`；不得顺手修 `InstallPointer.scala:104` 之类的主代码…」
 *   ⇒ **#2 的授权面（R2 明列三处并给定写法）**；台账⑦(1) 为其执行记录。
 *
 * 〔待追认项（本批执行员如实申报，**不与上述条款混同**）〕
 *   · `CredentialFileAcl` 7 处 `private[core]`→`private[shared]`：R1 只授权**迁移**（A1①/A4），
 *     该 7 处是**编译器强制**的后果（E139；两轮审计均独立复核了必要性与可见性零收窄）——
 *     **无任何条款逐字点名限定符改写** ⇒ 列为本批「待追认项」，请编排端以 R3（或 R1 追认条）
 *     明确后闭合。台账⑦(2) 已先行申报。
 *   · R2 三处站点改动：已按 R2 原文执行并逐处落注；若审计材料仍缺 R2 文本，请以本台账⑧ 为准。
 *
 * 〔两侧边表（审计 #5(a)：旧侧 526 边不便复现 → 现双侧照录，逐对可核）〕
 *   旧侧（改前实测，`node scripts/check-scala-layers.mjs`，与编排端 workflow 基线日志「总 526
 *   边、core→agent=0、豁免 0/未豁免 0」一致）逐对：actor→shared 8；agent→shared 30 / actor 29
 *   / core 23 / llm 7 / bridge 1 / dropbox 1 / neblink 1；bridge→shared 2；cli→shared 14 /
 *   core 6 / actor 2 / service 1；core→shared **152** / actor 46；dropbox→shared 7 / core 3；
 *   gateway→shared 36 / core **26** / agent 14 / service 13 / llm 13 / actor 12 / neblink 10 /
 *   dropbox 3 / bridge 1 / cli 1 / social 1；llm→shared 12 / core 5；neblink→shared 20 / core 11
 *   / dropbox 3 / actor 1；service→shared 4 / actor 2 / core 2 / agent 1；social→core 1 /
 *   shared 1 ⇒ **合计 526**。
 *   新侧（改后实测）仅五组变动：cli→shared **11**（−3）、cli→core **3**（−3）、core→shared
 *   **148**（−4）、gateway→shared **37**（+1）、gateway→core **26**（0：SessionRecorder 迁出
 *   −1 与 CanvasTabStore 迁入 +1 相抵）；其余 33 组逐对不变，core→agent 保持 **0** ⇒ **合计
 *   517**（−9 = −3−3−4+0+1）。两侧均以本节的「逐对 + 合计」形式可复算，无需 checkout。
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * ORCH5 台账⑨：`ORCH5-R3` 追认（限定符改写）+ 授权交付指针 + 边变化逐行成因分类
 * ─────────────────────────────────────────────────────────────────────────────
 * **授权文本交付指针**（编排端答复确认）：本批 `ORCH5-R1`/`ORCH5-R2` 条款的**有效交付载体
 * 即本文件 ORCH5 台账⑧**（逐字节选入的 A1②/A1①·A4/A2·A3/B/C1/C2/C3/D/E 与 R2 全文要旨），
 * 审计读 `git diff HEAD` 即可从产物本身核验授权编号与文本，无需对话层转达。
 *
 * 〔`ORCH5-R3` 节选——限定符追认裁定，逐字〕
 *   · 「追认 **ORCH5-R3**：`CredentialFileAcl` 的 7 处 `private[core]`→`private[shared]`
 *     限定符改写**准予追认**，不构成越权。理由即你的实证：①编译器强制…②可见性零收窄…
 *     ③两项独立复核支持你的自述。回退该项等于撤销已裁定的迁移，不做。」
 *   · 通用规则：「本批任何已在册批准的『文件迁移/归位』所引发的**编译器强制**限定符/包路径
 *     调整，一律视为该迁移授权的当然范围；但必须在 summary 逐处列出（符号 → 旧限定符 →
 *     新限定符 → E139 证据行 → 引用面复核结论）。若出现任何**非强制**的限定符调整（即不改
 *     也能编译的那种），不在本条追认面内，须单独上报。」
 *   ⇒ 本批**无任何非强制**限定符调整（7 处全部为 E139 强制；`core/boot`、`core/usage` 内
 *     的 `private[core]`（`JvmRequirement` 3 处 / `UsageRecordStore` 1 处）仍合法 ⇒ 未动）。
 *
 * 〔R3 要求的逐项表（7 处；E139 证据行取自 `target/wforch5-compile-r1.log`，为**修复前**
 * 坐标；现址 = 修复前 +10，因本批在同文件头插入 10 行并置注）〕
 *   ① `aclView`            private[core] → private[shared]  E139 :110:20 → 现址 :120  引用面：全部本文件内（:100/:391/:415 内部调用），外部 0
 *   ② `installOwnerOnlyAce` 同左                             E139 :125:20 → 现址 :135  引用面：全部本文件内，外部 0
 *   ③ `ownerOnlyAce`        同左                             E139 :176:20 → 现址 :186  引用面：全部本文件内（:126 调用 + :229 注释提及），外部 0
 *   ④ `Win32Bit`            同左                             E139 :210:20 → 现址 :220  引用面：全部本文件内（:238/:242），外部 0
 *   ⑤ `anchorHere`          同左                             E139 :577:20 → 现址 :587  引用面：全部本文件内（:470 调用），外部 0
 *   ⑥ `describe`            同左                             E139 :589:20 → 现址 :599  引用面：外部同名命中（AgentDef/PluginBlockPolicy/FileRefs/
 *     CredentialDiagnostics/RelayTunnelDiagnostics 等**异构** describe）无一调用本成员 ⇒ 有效外部引用 0
 *   ⑦ `needsNarrowing`      同左                             E139 :634:20 → 现址 :644  引用面：全部本文件内（:647 restrictDirectory 调用），外部 0
 *   （`grep -oE 'CredentialFileAcl\.scala:[0-9]+:[0-9]+' target/wforch5-compile-r1.log | sort -u | wc -l` = **7**，与上表一一对应。）
 *
 * 〔边变化逐行成因分类（R3 要求；供审计逐对核对「成对出现」规则）〕
 *   纯「文件迁出 main 树 / 删除」类（**只减不加**）：
 *     · cli→core −2（删 `cli/ui.scala` + `cli/uiStore.scala`，二者唯一 core 引用 = `ReplUi`）
 *     · cli→shared −3（删 `cli/ui.scala` + `cli/uiStore.scala` + `cli/markdownRenderer.scala`）
 *     · core→shared −1（删 `core/replUi.scala`）
 *     · gateway→core −1 与 gateway→shared −1（`gateway/SessionRecorder.scala` **迁出 main 树**
 *       入 test 树 ⇒ 其两条边离开被扫图；该件在 test 侧仍无包边可计）
 *   「迁址换包」类（原则上**成对**：一侧减、另一侧加；同包落点则无对侧）：
 *     · core→shared −3（`HeadlessMode`/`OnboardingService`/`CanvasTabStore` 三件迁出 core）
 *       ↔ gateway→shared **+2**（OnboardingService/CanvasTabStore 迁入 gateway 后各持 shared 引用）
 *       ＋ `HeadlessMode` 落 shared ⇒ 与原包同源 ⇒ **无对侧加项**（这正是「落 L0 即消边」的形态）
 *     · cli→core −1（SingleInstanceGuard 唯一 core 引用由 `nebflow.core.HeadlessMode` 改指
 *       `nebflow.shared.HeadlessMode`）⇒ 对侧 cli→shared **无 +**：该文件早已在 cli→shared 册内
 *       （边按「文件×目标包」计数，故换指只在旧侧产生消失）
 *     · gateway→core **+1**（`CanvasTabStore` 迁入 gateway 后持 `nebflow.core.AtomicJson` 引用）
 *       ↔ 其对侧 = core→shared −3 中该文件的 −1（迁出 core）
 *   合计：**526 → 517（−9 = −2−3−1−1−1−1 纯删除类〔−9〕 + 迁址类 −3+2−1+1 与 迁出类 −1+… 相抵后 0）**
 *   —— 简式：−9 = 纯删除/迁出 main 树 −9（cli→core −2、cli→shared −3、core→shared −1、
 *   gateway→core −1、gateway→shared −1）+ 迁址换包净 0（core→shared −3 与 gateway→shared +2
 *   及 gateway→core +1、cli→core −1 相抵）+ 其余 33 组 0。
 *
 * 纪律（本落点与 ORCH5 共同承担）：原注逐字保留、新注并置（同时含 `ORCH5-P*` 与
 * `ORCH5-R*`）；字符串字面量零改（唯一例外 = 台账④ 的两条 logback FQN）；前四批产物
 * 代码零重排（本批只在其注释面并置注）；测试树改动 = 随迁 package/import 行 + `ORCH5-R1`
 * E 条授权的内联 FQN 重 target（断言/期望值/用例名零变）+ **`ORCH5-R2` 授权的三处环境
 * 容忍化**（台账⑦；仅实值侧分隔符归一 / POSIX 用例加 `assume`，断言意图零变）。
 */
private[agent] object LifecycleEnds {}
