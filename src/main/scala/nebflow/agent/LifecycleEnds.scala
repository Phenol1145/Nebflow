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
 */
private[agent] object LifecycleEnds {}
