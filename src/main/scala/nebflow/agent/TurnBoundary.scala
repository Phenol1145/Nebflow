package nebflow.agent

import cats.effect.IO
import io.circe.JsonObject
import nebflow.actor.*
import nebflow.shared.*

/**
 * turn 边界协议对象（编排收敛第一批 ORCH1，2026-09-27 裁定批 ORCH1-R1..R12）：
 * 注入发射的调用面组装 + 判源守卫、三队列（pendingUserInputs / pendingEvents /
 * pendingImmediateInputs）的入队 / 取队首 / 跨边界携带、drain 决策的按边界种类
 * 命名入口、idle 组装（P4 toIdle）收进本单一对象；行为逐字节保持（帧字段、
 * 队列语义、注释均自原站点逐字迁移）。
 *
 * pi 式扩展 Phase 1 事件总线挂点（2026-09-27，ORCH1-P5 裁定）：本对象的事件面
 * **只允许**类型化结构与命名方法——禁止引入任何事件分发 / 回调 / 注册行为；
 * 未来事件总线接线只在本单点挂接，站点侧零改动。
 *
 * 帧构造唯一实现仍留驻 `AgentActor#emitInjectedUserEvent`（def 本体不动，
 * ORCH1-R6 裁定：本批的「唯一实现」= 调用面组装 + 守卫收口，禁转发别名）；
 * 纯 drain 决策件 = `TurnBoundaryDrains`（不动，ORCH1-R11 裁定：本对象只做按
 * 边界种类的命名入口，实参按站传入）。
 */
private[agent] object TurnBoundary:

  // ============================================================
  // 注入发射唯一组装（ORCH1-R1/R2/R4/R5）—— 站点调用面收口
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH1-R1 P1 对1）：AgentIdle SkillActivate 唤醒腿与
   * AgentFrozen SkillActivate 唤醒腿两处调用逐字同形 ⇒ 统一改指本方法；
   * 帧字面量 "skill" / None / sessionProject 组装随迁零改动。
   */
  private[agent] def emitForSkill(
    resources: SharedResources,
    state: AgentState,
    input: String
  )(using ctx: ActorContext[AgentCommand]): IO[Unit] =
    AgentActor.emitInjectedUserEvent(
      resources,
      state.wsSend,
      state.sessionId,
      input,
      "skill",
      None,
      sessionProject = state.projectName
    )

  /**
   * 2026-09-27 裁定（ORCH1-R1 P1 对3）：AgentActor#emitInjectedBubbles 的
   * immediate-input 腿与 AgentProcessing ToolsComplete 注入腿两处调用逐字
   * 同形（守卫 injectionSourceFor match、None ⇒ IO.unit 均入本方法，R2 同款
   * 守卫收口方式）⇒ 统一改指本方法。
   */
  private[agent] def emitForImmediateInput(
    resources: SharedResources,
    state: AgentState,
    imm: AgentCommand.ImmediateInput
  )(using ctx: ActorContext[AgentCommand]): IO[Unit] =
    AgentActor.injectionSourceFor(imm.fromUser, imm.source) match
      case Some(src) =>
        AgentActor.emitInjectedUserEvent(
          resources,
          state.wsSend,
          state.sessionId,
          imm.text,
          src,
          imm.eventType,
          imm.sender,
          imm.senderTeam,
          imm.delivery,
          // 气泡四段式统一批（2026-09-15）：PROJECT 段链首级随件转发（发送方所属
          // 项目），② 级 = 本会话所属项目。
          project = imm.project,
          sessionProject = state.projectName
        )
      case None => IO.unit

  /**
   * 2026-09-27 裁定（ORCH1-R5）：AgentActor#emitInjectedBubbles 的
   * queued-UserInput 腿（全元数据 intake/project 形态）逐字入本方法。
   */
  private[agent] def emitForQueuedUserCommand(
    resources: SharedResources,
    state: AgentState,
    ui: AgentCommand.UserInput
  )(using ctx: ActorContext[AgentCommand]): IO[Unit] =
    AgentActor.injectionSourceFor(ui.fromUser, ui.source) match
      case Some(src) =>
        AgentActor.emitInjectedUserEvent(
          resources,
          state.wsSend,
          state.sessionId,
          ui.text,
          src,
          ui.eventType,
          ui.sender,
          ui.senderTeam,
          ui.delivery,
          // 收件判别字段随 UserInput 同源转发（mailbadge 批 2026-09-13）。
          intake = ui.intake,
          // 气泡四段式统一批（2026-09-15）：PROJECT 段链首级/② 级（同上）。
          project = ui.project,
          sessionProject = state.projectName
        )
      case None => IO.unit

  /**
   * 2026-09-27 裁定（ORCH1-R2，P2 对2）：AgentIdle ExternalEvent 收件气泡腿与
   * AgentProcessing ExternalEvent 收件气泡腿统一改指本方法（visibleExternalEventSource
   * 的 match 守卫收进方法内，None ⇒ IO.unit 保持）。waitingForBatch 恒 false 不设
   * 参数——证据链（普查实读）：AgentIdle 侧唯一经由 `receiveVisibility(waiting)`
   * 的三处调用全部传 `waiting = false`（AgentIdle.scala :369/:395/:435），
   * AgentProcessing 侧调用本不传 ⇒ 两侧发射帧逐字节一致，合并行为保持。
   */
  private[agent] def emitForExternalEvent(
    resources: SharedResources,
    state: AgentState,
    source: String,
    eventType: String,
    payload: String,
    metadata: JsonObject
  )(using ctx: ActorContext[AgentCommand]): IO[Unit] =
    AgentActor.visibleExternalEventSource(source, eventType) match
      case Some(s) =>
        val agentName = metadata("agentName").flatMap(_.asString)
        AgentActor.emitInjectedUserEvent(
          resources,
          state.wsSend,
          state.sessionId,
          payload,
          s,
          Some(eventType),
          agentName,
          sessionProject = state.projectName
        )
      case None => IO.unit

  /**
   * 2026-09-27 裁定（ORCH1-R4）：AgentFinishTurn 的 mail-queue 排空腿纯搬名
   * ——字面量 "mail-queue" / Some("queue") / None / Some("queue") 序列零改动。
   */
  private[agent] def emitForMailQueueLegacy(
    resources: SharedResources,
    state: AgentState,
    message: String,
    from: String
  )(using ctx: ActorContext[AgentCommand]): IO[Unit] =
    AgentActor.emitInjectedUserEvent(
      resources,
      state.wsSend,
      state.sessionId,
      message,
      "mail-queue",
      Some("queue"),
      Some(from),
      None,
      Some("queue"),
      // 气泡四段式统一批（2026-09-15）：legacy 排空腿的 source =
      // `"mail-queue"` 不在 KIND 词表内 ⇒ 本腿恒不产 header（旧呈现
      // 逐字保持）。PROJECT 段落回级别照传，未来若纳入词表即生效。
      sessionProject = state.projectName
    )

  /**
   * 2026-09-27 裁定（ORCH1-R7，P1 四重同形）：ImmediateInput → User message 的
   * 四处逐字同形转换（原 AgentActor#immInputToMessage:259-263、
   * AgentFinishTurn:477-482、AgentProcessing:250-255、AgentProcessing:580-585）
   * 收进本单点；原 AgentActor 局部 helper 真删除（禁 re-export shim）。
   */
  /** ImmediateInput → User message (blocks preferred, text fallback). */
  private[agent] def immediateInputToMessage(imm: AgentCommand.ImmediateInput): Message =
    (imm.blocks match
      case Some(blocks) if blocks.nonEmpty => Message(MessageRole.User, Right(blocks))
      case _ => Message(MessageRole.User, Left(imm.text))
    ).copy(source = AgentActor.injectionSourceFor(imm.fromUser, imm.source))

  // ============================================================
  // idle 直投腿判源（ORCH1-R5）
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH1-R5）：以下 inferInjectionSource 的函数体与大注释
   * 自 AgentIdle 逐字迁入（一字未改），旧局部 helper 真删除；对外判源 =
   * userInputInjectionSource（clientMessageId / injectionSourceFor 双判保持
   * 原 AgentIdle:102-104 原形）。
   */
  /**
   * Infer the injection source (任务 P) for a tool-originated UserInput that
   * did not carry an explicit source. Discriminator: clientMessageId is empty
   * (user WS inputs always carry one). The source is derived from the agent's
   * own session id prefix, falling back to the fork-adapter replyTo for
   * Mail ask/fork spawns, and finally the generic "tool".
   *
   *   delegate-… → "delegate"   DelegateTool 内核会话（一次性执行件）
   *   subtask-…  → "subtask"    SubTaskTool worker
   *   dag-…      → "flow"       FlowDagExecutor node
   *   otherwise  → "tool"
   */
  private def inferInjectionSource(
    sessionId: Option[String],
    replyTo: Option[ActorRef[AgentEvent]]
  ): Option[String] =
    val sid = sessionId.getOrElse("")
    if sid.startsWith("delegate-") then Some("delegate")
    else if sid.startsWith("subtask-") then Some("subtask")
    else if sid.startsWith("dag-") then Some("flow")
    else Some("tool")

  /** 2026-09-27 裁定（ORCH1-R5）：AgentIdle UserInput 直投腿的判源收口。 */
  private[agent] def userInputInjectionSource(
    clientMessageId: Option[String],
    fromUser: Boolean,
    source: Option[String],
    sessionId: Option[String],
    replyTo: Option[ActorRef[AgentEvent]]
  ): Option[String] =
    if clientMessageId.isDefined then None
    else AgentActor.injectionSourceFor(fromUser, source.orElse(inferInjectionSource(sessionId, replyTo)))

  // ============================================================
  // 三队列唯一操作面（ORCH1-R8）
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH1-R8）：pendingUserInputs 入队单点
   * （AgentProcessing UserInput/SkillActivate/AskQuestion 三缓冲腿、
   * AgentFrozen 系统注入排队腿改指；追加语义 `:+` 随迁）。
   */
  private[agent] def enqueueUserInput(exec: ExecutionContext, cmd: AgentCommand): ExecutionContext =
    exec.copy(pendingUserInputs = exec.pendingUserInputs :+ cmd)

  /**
   * 2026-09-27 裁定（ORCH1-R8）：pendingImmediateInputs 入队单点
   * （AgentProcessing 入队腿、AgentFrozen 排队腿改指）。
   */
  private[agent] def enqueueImmediateInput(
    exec: ExecutionContext,
    imm: AgentCommand.ImmediateInput
  ): ExecutionContext =
    exec.copy(pendingImmediateInputs = exec.pendingImmediateInputs :+ imm)

  /**
   * 2026-09-27 裁定（ORCH1-R8）：pendingEvents 入队 + sub-agent barrier 递减
   * 单点（普查证 AgentProcessing:1056-1058 与 AgentFrozen:559-561 两处递减逐字
   * 同形 ⇒ 并入本方法；completed OR failed 均满足一个在飞槽位，队列 append
   * 保到达顺序）。
   */
  private[agent] def enqueueExternalEvent(
    exec: ExecutionContext,
    event: AgentCommand.ExternalEvent
  ): ExecutionContext =
    val isSubagentResult = TurnBoundaryDrains.isSubagentResult(event)
    val outstanding = exec.outstandingSubagentResults
    val newOutstanding = if isSubagentResult then math.max(0, outstanding - 1) else outstanding
    exec.copy(
      pendingEvents = exec.pendingEvents :+ event,
      outstandingSubagentResults = newOutstanding
    )

  /**
   * 2026-09-27 裁定（ORCH1-R8）：F2 崩溃恢复预插单点（AgentIdle
   * RecoverPersistedQueues 腿改指；磁盘条目排在内存队列之前 = 原逐字语义）。
   */
  private[agent] def recoverPersistedQueues(
    exec: ExecutionContext,
    imms: List[AgentCommand.ImmediateInput],
    events: List[AgentCommand.ExternalEvent]
  ): ExecutionContext =
    exec.copy(
      pendingImmediateInputs = imms ++ exec.pendingImmediateInputs,
      pendingEvents = events ++ exec.pendingEvents
    )

  /**
   * 2026-09-27 裁定（ORCH1-R8）：idle 批完成汇聚清队单点（AgentIdle
   * 「batch complete ⇒ 释放全部 HELD 结果」腿改指；outstanding 归零 + 队列清空
   * = 原逐字语义）。
   */
  private[agent] def clearHeldEvents(exec: ExecutionContext): ExecutionContext =
    exec.copy(outstandingSubagentResults = 0, pendingEvents = Nil)

  /**
   * 2026-09-27 裁定（ORCH1-R8）：pendingUserInputs 取队首 + tail-safe 尾留存
   * 单点（AgentFinishTurn returnToIdle 的 head-forward 腿与 AgentProcessing
   * CompactionComplete 空注入分支 head-forward 腿改指；(head, 取队首后的 exec)
   * 一并返回，写点不再手工决定『是否带过边界』）。
   */
  private[agent] def forwardUserInputHead(
    exec: ExecutionContext
  ): (Option[AgentCommand], ExecutionContext) =
    // Empty-queue safe tail (same guard style as TurnBoundaryDrains.drainHead):
    // the REST /api/command turn-end path reaches this branch with an empty
    // queue on every turn — a bare .tail threw "tail of empty list" there.
    val queued = exec.pendingUserInputs
    val remaining =
      if queued.isEmpty then queued else queued.tail
    (queued.headOption, exec.copy(pendingUserInputs = remaining))

  /**
   * 2026-09-27 裁定（ORCH1-R8）：turn 边界 ExecutionContext.idle 重建 + 三队列
   * 跨边界携带单点——**全携带默认**（events/imms/users/outstanding/mailCount
   * 默认从 exec 原样带过），已消费侧与 owedCompletion 以显式覆盖参数传入
   * （2026-09-27 门禁修复：默认参数不得引用同列表前置参数——E006——
   * owedCompletion 改 Option 覆盖形，None ⇒ exec 原样带过，语义不变）。
   * 本批仅 AgentProcessing recoverable-abort 腿接入（无站点专属注释冲突）；
   * AgentFinishTurn 三注入腿各自带站点级日期裁定注释 ⇒ ORCH1-R8 待下批。
   */
  private[agent] def withCarriedQueues(
    exec: ExecutionContext,
    messages: List[Message],
    currentTurnId: Long = 0L,
    owedCompletionOverride: Option[List[ActorRef[AgentEvent]]] = None,
    drainedEvents: Option[List[AgentCommand.ExternalEvent]] = None,
    drainedImmediateInputs: Option[List[AgentCommand.ImmediateInput]] = None,
    remainingUserInputs: Option[List[AgentCommand]] = None,
    mailQueueCountDelta: Int = 0
  ): ExecutionContext =
    ExecutionContext
      .idle(messages, exec.turnIdx, currentTurnId)
      .copy(
        pendingEvents = drainedEvents.getOrElse(exec.pendingEvents),
        pendingImmediateInputs = drainedImmediateInputs.getOrElse(exec.pendingImmediateInputs),
        pendingUserInputs = remainingUserInputs.getOrElse(exec.pendingUserInputs),
        outstandingSubagentResults = exec.outstandingSubagentResults,
        owedCompletion = owedCompletionOverride.getOrElse(exec.owedCompletion),
        pendingMailQueueCount = exec.pendingMailQueueCount + mailQueueCountDelta
      )

  // ============================================================
  // idle 组装归口（ORCH1-R10，P4）
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH1-R10 / P4）：AgentFinishTurn returnToIdle 的手抄
   * 逐字段 ExecutionContext.idle copy 收口本方法——组装语义不变，原站点
   * :340-358 四段日期裁定注释逐字随迁（禁改）。users 尾 = 调用方经
   * forwardUserInputHead 取队首后的 exec.pendingUserInputs。
   */
  private[agent] def toIdle(
    exec: ExecutionContext,
    messages: List[Message],
    owedCompletion: List[ActorRef[AgentEvent]]
  ): ExecutionContext =
    ExecutionContext
      .idle(messages, exec.turnIdx)
      .copy(
        interaction = exec.interaction.filter(_.pendingPermission.isDefined),
        // Preserve queue when compaction is in progress — CompactionComplete drains it.
        pendingImmediateInputs = exec.pendingImmediateInputs,
        pendingUserInputs = exec.pendingUserInputs,
        // Sub-agent barrier: held subtask/delegate results and the
        // outstanding count survive the turn boundary (the batch may
        // still be running — they are injected when it completes).
        pendingEvents = exec.pendingEvents,
        outstandingSubagentResults = exec.outstandingSubagentResults,
        // #25: parked completion debt survives the turn boundary —
        // paid off when a later turn ends with the barrier at 0.
        owedCompletion = owedCompletion,
        // #10 (2026-08-27 mail-queue wedge): ExecutionContext.idle
        // rebuilds pendingMailQueueCount to 0. This returnToIdle tail
        // is ALSO reached by the queue drain branch's idle-gate
        // deferral path (pendingMailQueueCount>0 but subtree busy) —
        // without carrying the counter, the NEXT turn's drain branch
        // (`pendingMailQueueCount > 0`) is permanently false and the
        // disk queue wedges until another MailQueued arrives. The two
        // earlier injection branches already carry it; this tail must too.
        pendingMailQueueCount = exec.pendingMailQueueCount
      )

  // ============================================================
  // drain 决策归口（ORCH1-R11）—— 按边界种类命名，实参按站传入，
  // 内部仍调 TurnBoundaryDrains 纯函数（决策件不动）
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH1-R11）：finishTurnCont 事件边界（原 AgentFinishTurn:379-384）
   * ——compaction 此刻不可能在飞（分支前提），drainBarrier 实参 compactionPending =
   * false 按站传入。
   */
  private[agent] def drainForFinishTurnEvents(
    state: AgentState
  ): (List[AgentCommand.ExternalEvent], List[AgentCommand.ExternalEvent]) =
    TurnBoundaryDrains.drainBarrier(
      state.execution.pendingEvents,
      compactionPending = false,
      state.execution.outstandingSubagentResults
    )

  /**
   * 2026-09-27 裁定（ORCH1-R11）：finishTurnCont immediate-input 边界（原
   * AgentFinishTurn:452-455）——分支前提已排除 compaction ⇒ compactionPending =
   * false 按站传入。
   */
  private[agent] def drainForFinishTurnImm(
    state: AgentState
  ): (Option[AgentCommand.ImmediateInput], List[AgentCommand.ImmediateInput]) =
    TurnBoundaryDrains.drainHead(
      state.execution.pendingImmediateInputs,
      compactionPending = false
    )

  /**
   * 2026-09-27 裁定（ORCH1-R11）：ToolsComplete 事件边界（原
   * AgentProcessing:544-549）——压缩在飞时整队 HELD（save/compact 轮历史会被
   * summary 替换），compactionPending = state.pendingCompaction.isDefined 按站传入。
   */
  private[agent] def drainForToolsCompleteEvents(
    state: AgentState
  ): (List[AgentCommand.ExternalEvent], List[AgentCommand.ExternalEvent]) =
    TurnBoundaryDrains.drainBarrier(
      state.execution.pendingEvents,
      state.pendingCompaction.isDefined,
      state.execution.outstandingSubagentResults
    )

  /**
   * 2026-09-27 裁定（ORCH1-R11）：ToolsComplete immediate-input 边界（原
   * AgentProcessing:566-567）——压缩在飞时保持排队，CompactionComplete 再排空。
   */
  private[agent] def drainForToolsCompleteImm(
    state: AgentState
  ): (Option[AgentCommand.ImmediateInput], List[AgentCommand.ImmediateInput]) =
    TurnBoundaryDrains.drainHead(state.execution.pendingImmediateInputs, state.pendingCompaction.isDefined)

  /**
   * 2026-09-27 裁定（ORCH1-R11）：recoverable-abort 恢复边界（原
   * AgentProcessing:245-248）——abort 的目的就是让排队输入进来 ⇒
   * compactionPending = false 按站传入。
   */
  private[agent] def drainForRecoverableAbortImm(
    state: AgentState
  ): (Option[AgentCommand.ImmediateInput], List[AgentCommand.ImmediateInput]) =
    TurnBoundaryDrains.drainHead(
      state.execution.pendingImmediateInputs,
      compactionPending = false
    )

  // ============================================================
  // 完成判定归口（ORCH2-P4）—— turn 完成协作的**纯判定**单点；
  // 写入面（TeamSessionRegistry.markBusy/markIdle、AgentRegistryEmit
  // 的 modify 本体）与既有唯一实现（MailIdleGate.isAgentTreeIdle 调用面、
  // AgentFinishTurn.fullyIdle 的 IO 取数面）不动；fullyIdle 的组合判定经
  // 门禁审计定案收口本节 mailDrainGateIdle——本节零 core 引用（本对象
  // 原有 agent→core 包边数不变：门禁包边预算仅允许 →shared/→actor）。
  // 判定皆为纯函数——不触本对象头注的事件总线挂点纪律（禁分发/回调）。
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH2-P4）：markTeamBusy/markTeamIdle 的判定条件收口
   * （原形 = agentDef.category == "team" 门 + sid 存在性折叠，真值表逐字
   * 等价）；两方法的写入体与 AgentActor 委托面零触碰。
   */
  // 2026-09-27 裁定（ORCH3-R1 / ORCH3-P1，适用预批 P1）：T4 收面撤销前条保留——委托 def 已删除，调用点改指 AgentFinishTurn.markTeamBusy / AgentFinishTurn.markTeamIdle；原注保留存证。
  private[agent] def teamMarkEligible(agentDef: AgentDef, sid: Option[String]): Boolean =
    agentDef.category == "team" && sid.nonEmpty

  /**
   * 2026-09-27 裁定（ORCH2-P4）：touchRegistryActivity 的 turn 起点相位判定
   * 收口本纯函数；AgentRegistryEmit 的 modify 写入本体（唯一写面）其余逐字
   * 不动。原站点判定注释逐字随迁下行。
   */
  // turn 起点：仅当本会话此前不在 Processing（WaitingForUser 是同一
  // turn 内的人机交互子态，不算新 turn 起点）。
  private[agent] def isTurnStartTransition(prevStatus: AgentStatus, turnStart: Boolean): Boolean =
    turnStart && prevStatus != AgentStatus.Processing && prevStatus != AgentStatus.WaitingForUser

  /**
   * 2026-09-27 裁定（ORCH2-P4）：「离开 Processing 一律清工具相位」判定单点
   * （2026-09-10 卡死判据换轴的方法级语义注留守 AgentRegistryEmit 头注）。
   */
  private[agent] def keepsToolPhase(status: AgentStatus): Boolean =
    status == AgentStatus.Processing

  /**
   * 2026-09-27 裁定（ORCH2-P4）：finishTurnCont 完成债三要素判定收口（原
   * AgentFinishTurn 的 completionTargets / subagentsInFlight / owedAfter 三
   * val 逐字等价，#25 裁定块随迁下行，站点留指向注）。返回
   * (completionTargets, subagentsInFlight, owedAfter)。
   */
  // #25 (nested delegation dead-letter): "turn ended" is NOT "task completed"
  // while spawned sub-agents are still in flight. The completion notification
  // (supervisor adapter / ask fork / flow bridge) is PARKED in
  // execution.owedCompletion instead of being sent — BackoffSupervisor would
  // otherwise stop this actor on Completed and the grandchildren's results
  // would dead-letter. When a later turn ends with the barrier at 0, every
  // parked target receives the Completed event carrying the FINAL synthesized
  // text and the debt clears. Root agents (replyTo=None, no debt) unaffected.
  private[agent] def turnEndCompletionDebt(
    replyTo: Option[ActorRef[AgentEvent]],
    exec: ExecutionContext
  ): (List[ActorRef[AgentEvent]], Boolean, List[ActorRef[AgentEvent]]) =
    val completionTargets: List[ActorRef[AgentEvent]] = (replyTo.toList ++ exec.owedCompletion).distinct
    val subagentsInFlight = exec.outstandingSubagentResults > 0
    val owedAfter: List[ActorRef[AgentEvent]] = if subagentsInFlight then completionTargets else Nil
    (completionTargets, subagentsInFlight, owedAfter)

  /**
   * 2026-09-27 裁定（ORCH2-P4，经门禁审计定案补做；同轮按门禁包边记账反馈修复）：
   * #407 mail-queue 空闲 gate 投递判定（fullyIdle 的判定条件）收口本纯函数——
   * internalIdle（agent 内部权威 barrier，registry 快照之外的一层防御，行注逐字
   * 随迁）与树空闲实参 treeIdle（= MailIdleGate.isAgentTreeIdle 既有唯一实现的
   * 调用结果——调用与数据取数留守 AgentFinishTurn.fullyIdle 站点：该站点本已
   * 持有 agent→core 包边，本对象**不因归口新增 core 引用**，本批包边预算仅
   * 允许 →shared/→actor 记账增长）的 && 组合序原样（selfOk && internalIdle）；
   * skipSelfStatus→checkStatus 取反语义（turn 已结束时自身 status 为 Processing
   * 残留、不算忙——2026-08-28 01:00 统一裁定注原样留守 AgentFinishTurn 站点）
   * 随调用点实参保持。纯 Boolean 组合，无副作用/短路可观测差异 ⇒ 行为逐字不变；
   * 委托链 AgentActor.fullyIdle 零触碰，无转发 shim。
   */
  // 2026-09-27 裁定（ORCH3-R1 / ORCH3-P1，适用预批 P1）：T4 收面撤销前条保留——委托 def 已删除，调用点改指 AgentFinishTurn.fullyIdle；原注保留存证。
  private[agent] def mailDrainGateIdle(treeIdle: Boolean, state: AgentState): Boolean =
    // agent 内部权威 barrier（registry 快照之外的一层防御）
    val internalIdle = state.execution.outstandingSubagentResults == 0
    treeIdle && internalIdle

end TurnBoundary
