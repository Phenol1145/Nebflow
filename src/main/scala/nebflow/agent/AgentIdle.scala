/* 从 AgentActor 迁出(行为保持重构,2026-09-25)。 */
package nebflow.agent

import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import io.circe.syntax.*
import nebflow.actor.*
import nebflow.core.*
import nebflow.core.ask.AskService
import nebflow.core.compact.*
import nebflow.shared.{NebflowLogger, *}

/**
 * idle 域:原 object AgentActor 内 idle 行为的实现整体迁入本件(方法体逐字
 * 未动;idle 内部的自递归——返回下一 idle behavior——保持原逻辑,解析到本
 * object 的 idle)。AgentActor 侧保留同名委托 def(签名原样),全部调用点
 * (apply 入口 / processing / AgentFrozen 等)零改动。idle 唯一消费的注入判源
 * 助手 inferInjectionSource 随迁本件;touchBarrierSnapshot /
 * visibleExternalEventSource / buildEventReminder / emitInjectedUserEvent /
 * emitSessionBusy / pipeLlmCall 等与 processing/frozen 共用的助手留守
 * AgentActor,经 import AgentActor.* 引用(本 object 只含 def,无顶层状态)。
 */
// 2026-09-27 裁定（ORCH3-R1 / ORCH3-P1，适用预批 P1）：T4 收面撤销前条保留——委托 def 已删除，调用点改指 AgentIdle.idle；原注保留存证。
private[agent] object AgentIdle:
  import nebflow.agent.AgentActor.*

  // 2026-09-27 裁定（ORCH1-R5）：原局部助手 inferInjectionSource（含其任务 P 大
  // 注释）函数体逐字迁往 TurnBoundary（旧局部 helper 真删除，禁 re-export shim）；
  // idle UserInput 直投腿判源改指 TurnBoundary.userInputInjectionSource。

  // ============================================================
  // Idle state
  // ============================================================

  // idle 态已整体迁至 agent/AgentIdle.scala(行为保持重构,2026-09-25):idle
  // 行为及其唯一消费的注入判源助手 inferInjectionSource 的实现都在那边
  // (方法体逐字未动);此处保留同名委托 def(签名原样),调用点零改动。
  // 2026-09-27 裁定（ORCH3-R1 / ORCH3-P1，适用预批 P1）：T4 收面撤销前条保留——委托 def 已删除，调用点改指 AgentIdle.idle；原注保留存证。
  private[agent] def idle(
    agentDef: AgentDef,
    resources: SharedResources,
    depth: Int,
    parentRef: Option[ActorRef[AgentCommand]],
    state: AgentState
  )(using ctx: ActorContext[AgentCommand]): Behavior[AgentCommand] =
    Behaviors.receiveMessage:

      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 UserInput = 直投开轮
      // （dedup/语言检测/chatWidth/判源/Busy/注入事件/pipeLlmCall），与
      // processing 缓冲腿、frozen 唤醒/排队腿真语义差异 ⇒ 整站留（相同碎片
      // 量过小不提取；判源面已 ORCH1-R5 收口）。
      case AgentCommand.UserInput(
            text,
            replyTo,
            clientMessageId,
            blocks,
            chatWidth,
            source,
            sender,
            senderTeam,
            delivery,
            eventType,
            intake,
            fromUser,
            project
          ) =>
        val (isDuplicate, dedupedState) = checkDuplicate(clientMessageId, state)
        if isDuplicate then
          logger.info(s"Dropping duplicate message with clientMessageId=${clientMessageId.getOrElse("")}")
          IO.pure(idle(agentDef, resources, depth, parentRef, state))
        else
          logAgentEvent(agentDef, depth, state.sessionId, state.sessionName, "start", s"msgs=${state.messages.size}")
          val stateWithLang =
            if depth == 0 && parentRef.isEmpty && dedupedState.messages.isEmpty then
              LanguageDetector.detect(text) match
                case Some(lang) =>
                  NebflowLogger.forName("nebflow.agent").info(s"Auto-detected language: $lang")
                  dedupedState.withLanguage(Some(lang))
                case None => dedupedState
            else dedupedState
          val stateWithWidth =
            if chatWidth > 0 then stateWithLang.copy(session = stateWithLang.session.copy(chatWidth = chatWidth))
            else stateWithLang
          // Injection source (任务 P): user WS inputs always carry clientMessageId.
          // Tool-injected inputs (Mail ask/fork, Delegate, SubTask, ImmediateInput
          // converted from Mail delivery) have clientMessageId=None → emit a user
          // event so the frontend renders the task prompt as a visible bubble.
          // ② (2026-09-11): `fromUser` is the second, explicit discriminator —
          // a real human text that travelled the ImmediateInput leg arrives with
          // clientMessageId=None and used to be stamped source="tool" (blue TOOL
          // card + isRealUserTurn=false). 真人 ⇒ no source, never.
          // 2026-09-27 裁定（ORCH1-R5）：判源双守卫（clientMessageId ⇒ None /
          // injectionSourceFor(fromUser, source.orElse(infer)) 形态）收口 TurnBoundary。
          val injectionSource: Option[String] =
            TurnBoundary.userInputInjectionSource(clientMessageId, fromUser, source, state.sessionId, replyTo)
          val enrichedBlocks: Option[List[ContentBlock]] = blocks.filter(_.nonEmpty)
          val stateWithFlush = stateWithWidth
          val userMsg = (enrichedBlocks match
            case Some(bl) => Message(MessageRole.User, Right(bl))
            case None => Message(MessageRole.User, Left(text))
          ).copy(source = injectionSource)
          val newMessages = stateWithFlush.messages :+ userMsg
          val sessionBusyIO =
            if depth == 0 then
              stateWithWidth.sessionId.fold(IO.unit)(sid => emitSessionBusy(stateWithWidth.wsSend, sid, busy = true))
            else IO.unit
          val injectedEventIO = injectionSource match
            case Some(src) =>
              emitInjectedUserEvent(
                resources,
                stateWithWidth.wsSend,
                stateWithWidth.sessionId,
                text,
                src,
                eventType,
                sender = sender,
                senderTeam = senderTeam,
                delivery = delivery,
                // idle 直投腿（mailbadge 批）：收件判别字段随件同源落地。
                intake = intake,
                // 气泡四段式统一批（2026-09-15）：PROJECT 段链首级/② 级。
                project = project,
                sessionProject = stateWithWidth.projectName
              )
            case None => IO.unit
          for
            _ <- sessionBusyIO
            _ <- injectedEventIO
            result <- AgentProcessing.pipeLlmCall(
              agentDef,
              resources,
              depth,
              parentRef,
              stateWithFlush
                .withMessages(newMessages)
                .withEmptyResponseRetries(0)
                .withMailUsedThisTurn(false)
                // Block 3：新用户/系统 turn 起点——loop 纪元 +1
                .withNextLoopTurn,
              replyTo,
              // D2：真实用户 WS 输入恒带 clientMessageId → UserWake（冻结时段也
              // 放行，用户输入即唤醒）；系统注入（Mail/continue 等 None）→ Gated。
              if clientMessageId.isDefined then DispatchCause.UserWake else DispatchCause.Gated
            )
          yield result
          end for
        end if

      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 AskQuestion 直启 ask 轮，
      // frozen 态唤醒腿多 Resumed 帧 + frozenReason 清位 + resetCrossTurn——
      // 真语义差异 ⇒ 整站留。
      case AgentCommand.AskQuestion(question, askSessionId) =>
        logAgentEvent(agentDef, depth, state.sessionId, state.sessionName, "ask-start", s"q=${question.take(60)}")
        val askReminder = AskService.buildAskReminder(question)
        val askState = state
          .withMessages(state.messages :+ askReminder)
          .withAskMode(Some(question))
          .withStatus(AgentStatus.Processing)
          .withNextLoopTurn
        AgentProcessing.pipeLlmCall(agentDef, resources, depth, parentRef, askState, None, DispatchCause.UserWake)

      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 SkillActivate 带
      // sessionBusyIO2 直投腿，frozen 态唤醒腿为 Resumed 链 + resetCrossTurn——
      // 真语义差异 ⇒ 整站留（发射腿已 ORCH1-R1 归口 TurnBoundary.emitForSkill）。
      case AgentCommand.SkillActivate(skillName, input, skillSessionId, skillContent, skillBaseDir) =>
        logAgentEvent(
          agentDef,
          depth,
          state.sessionId,
          state.sessionName,
          "skill-start",
          s"skill=$skillName input=${input.take(60)}"
        )
        val combinedText = s"<skill name=\"$skillName\">\n$skillContent\n</skill>\n\n$input"
        val processingState = state
          .withMessages(
            state.messages :+ Message(MessageRole.User, Left(combinedText), source = Some("skill"))
          )
          .withStatus(AgentStatus.Processing)
          .withNextLoopTurn
        val sessionBusyIO2 =
          if depth == 0 then state.sessionId.fold(IO.unit)(sid => emitSessionBusy(state.wsSend, sid, busy = true))
          else IO.unit
        for
          _ <- sessionBusyIO2
          // 2026-09-27 裁定（ORCH1-R1 P1 对1）：与 AgentFrozen SkillActivate 腿逐字
          // 同形 ⇒ 统一改指 TurnBoundary.emitForSkill。
          _ <- TurnBoundary.emitForSkill(resources, state, input)
          result <- AgentProcessing.pipeLlmCall(
            agentDef,
            resources,
            depth,
            parentRef,
            processingState,
            None,
            DispatchCause.UserWake
          )
        yield result
        end for

      case AgentCommand.Interrupt() =>
        // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 Interrupt = no-op 留
        // 本态——与 processing/frozen 的「中断放弃续跑」腿（本批已收口
        // BehaviorCommon.interruptToIdle）真语义差异（无在飞 turn 可中断）⇒
        // 原样保留。
        IO.pure(idle(agentDef, resources, depth, parentRef, state))

      case AgentCommand.Stop(_) =>
        // 2026-09-27 裁定（ORCH2-P1）：三态 Stop 公共序列收口
        // BehaviorCommon.stopSequence；本态无 registry 中间腿 ⇒ IO.unit 恒值
        // 实参（参数化退化形态，行为逐字不变）；本态原无 logAgentEvent ⇒ 不补。
        BehaviorCommon.stopSequence(resources, state)(IO.unit)

      case AgentCommand.ClearReadTracker =>
        clearReadTrackerStay(state)(IO.pure(idle(agentDef, resources, depth, parentRef, state)))

      case AgentCommand.ResetSession =>
        // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本站与 processing/frozen 收敛
        // 面（AgentActor.resetSessionHandler）不同形——本态 cancelCurrentTurn 起手 /
        // 无 Interrupted 帧 / 无 registry 回写 / 无 resetToIdle(Nil) 清场（F 批
        // scaladoc 已裁定留守）⇒ 原样保留。
        for
          _ <- ctx.cancelCurrentTurn()

          _ <- state.readTracker.fold(IO.unit)(t => t.clear())
        yield
          val resetState = state
            .withMessages(Nil)
            .withLatestUsage(None)
            .withPendingCompaction(None)
            .withCompactionFailures(0)
            .withLastCompactionFailureAt(0L)
            .withRecentMessageIds(Nil)
            .invalidateSystemStableCache
          idle(agentDef, resources, depth, parentRef, resetState)

      case AgentCommand.TriggerCompaction(mode, replyDeferred, postCompactInstruction) =>
        AgentCompactionHandlers.handleTriggerCompaction(
          agentDef,
          resources,
          depth,
          parentRef,
          state,
          mode,
          replyDeferred,
          resumeAfterCompact = postCompactInstruction.isDefined,
          postCompactInstruction = postCompactInstruction
        )

      case AgentCommand.UpdateContextWindow(window) =>
        val newState = state.withContextWindow(window)
        val estimatedTokens = TokenEstimator.estimate(newState.messages)
        // ctxthresh 批：换模型/换窗口后的立即压缩判定同取**生效门限**——
        // 本会话有阈值覆盖时覆盖同样适用于新窗口（无覆盖时逐字 = 现值函数）。
        val threshold = newState.compactThresholdTokens
        if newState.messages.nonEmpty && estimatedTokens > threshold then
          logAgentEvent(
            agentDef,
            depth,
            state.sessionId,
            state.sessionName,
            "model-switch-compact",
            s"newWindow=$window estimated=$estimatedTokens threshold=$threshold msgs=${newState.messages.size}"
          )
          AgentCompactionHandlers.handleTriggerCompaction(
            agentDef,
            resources,
            depth,
            parentRef,
            newState,
            "full",
            None,
            resumeAfterCompact = false
          )
        else IO.pure(idle(agentDef, resources, depth, parentRef, newState))
        end if

      case AgentCommand.SetCompactThresholdRatio(ratio) =>
        // ctxthresh 批：只改存储值（口径④「有会话覆盖用覆盖，无覆盖走现值函数」）。
        // 🔴 刻意不做任何压缩触发——镜像 UpdateContextWindow 的存储语义但去掉其
        // 「顺手压缩」副作用（设计 §7 逐字禁止复用该命令的原因）。
        // 生效时点 = 下一回合边界（判定点 AgentCore 每轮读 state，非启动快照）。
        logAgentEvent(
          agentDef,
          depth,
          state.sessionId,
          state.sessionName,
          "compact-threshold-set",
          s"ratio=${ratio.map(r => f"$r%.4f").getOrElse("default")} " +
            s"effective=${CompactThresholdOverride.effectiveThreshold(state.contextWindow, ratio)}"
        )
        IO.pure(idle(agentDef, resources, depth, parentRef, state.withCompactThresholdRatio(ratio)))

      case n: AgentCommand.BackgroundTaskNotification =>
        forwardBackgroundTaskNotification(n)(IO.pure(idle(agentDef, resources, depth, parentRef, state)))

      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 ExternalEvent = barrier
      // 判定 + 立即注入开轮三腿（#418/批完成/单件），processing 态排队+气泡+
      // persist、frozen 态静默排队——真语义差异 ⇒ 整站留（收件气泡腿已 ORCH1-R2、
      // 清队/预插已 ORCH1-R8 单点）。
      case AgentCommand.ExternalEvent(source, eventType, payload, metadata, correlationId) =>
        logAgentEvent(
          agentDef,
          depth,
          state.sessionId,
          state.sessionName,
          "external-event",
          s"source=$source type=$eventType"
        )
        val event = AgentCommand.ExternalEvent(source, eventType, payload, metadata, correlationId)
        val sessionBusyIO =
          if depth == 0 then state.sessionId.fold(IO.unit)(sid => emitSessionBusy(state.wsSend, sid, busy = true))
          else IO.unit
        // 任务 Q: high-value result notifications (sub-agent/flow/background/api
        // completion) render as a visible injected-user bubble so the user can
        // see "what result arrived" in the chat stream, not just the LLM history.
        val visSource = visibleExternalEventSource(source, eventType)
        // P1.4: When a retryable sub-agent failure arrives, add a structured
        // system-reminder guiding the LLM to re-delegate — this is more
        // effective than relying on the LLM noticing the failure text alone.
        val retryableHint = eventType == "failed" &&
          metadata("retryable").exists(_.asBoolean.getOrElse(false))
        val reminderText =
          if retryableHint then
            val failureType = metadata("failureType").flatMap(_.asString).getOrElse("unknown")
            val failedSession = metadata("failedSessionId").flatMap(_.asString).getOrElse("")
            val agentName = metadata("agentName").flatMap(_.asString).getOrElse("")
            s"\n<system-reminder>\nA sub-agent task${if agentName.nonEmpty then s" ($agentName)" else ""}" +
              s"${if failedSession.nonEmpty then s" [session=$failedSession]" else ""} failed" +
              s" (failure type: $failureType). This is a retryable error — consider re-delegating" +
              s" the same task with the Delegate or SubTask tool.\n</system-reminder>"
          else ""
        val injectionText = payload + reminderText
        // Receive-time visibility (stream event + bubble) — mirrors the
        // processing-state path so the UI shows each result arriving even when
        // the LLM injection is held back by the batch barrier.
        def receiveVisibility(waiting: Boolean): IO[Unit] =
          emitStream(
            state.wsSend,
            AgentStreamEvent.ExternalEventReceived(source, eventType, correlationId),
            isSubagent = depth > 0,
            state.sessionId
          ) *> (
            // 2026-09-27 裁定（ORCH1-R2）：与 AgentProcessing ExternalEvent 收件气泡腿
            // 统一改指 TurnBoundary.emitForExternalEvent（visibleExternalEventSource
            // 守卫收进方法内）。waitingForBatch 恒 false——本方法三处调用
            // （:369/:395/:435 re-pin：行号随本批改指略有漂移）均传 waiting = false，
            // 证据链全文见 TurnBoundary 方法头裁定注释。
            TurnBoundary.emitForExternalEvent(resources, state, source, eventType, payload, metadata)
          )
        // ── Sub-agent result barrier (worker blocking semantics) ──────────
        // Delegate/SubTask results arriving while more of the same parallel
        // batch is still outstanding are HELD in pendingEvents instead of
        // interrupting the agent one result at a time. When the batch completes
        // (outstanding hits 0), ALL held results are injected together in a
        // single turn. Non-subagent events keep the existing immediate path.
        val isSubagentResult = TurnBoundaryDrains.isSubagentResult(event)
        val outstanding = state.execution.outstandingSubagentResults
        val newOutstanding = if isSubagentResult then math.max(0, outstanding - 1) else outstanding
        val held = state.execution.pendingEvents
        if isSubagentResult && newOutstanding > 0 then
          // #418: an idle parent must NEVER hold a sub-agent result back —
          // there is no in-flight turn to batch into, and holding it here
          // parks the parent asleep until user input (delegate COMPLETED
          // never wakes the parent — the reported bug). Inject THIS result
          // immediately to wake the parent; the rest of the batch stays HELD
          // in pendingEvents (injected together by the batch-complete branch
          // when they arrive), and the counter keeps the decremented
          // newOutstanding so the #25 nested-debt semantics hold.
          for
            _ <- sessionBusyIO
            _ <- receiveVisibility(waiting = false)
            _ <- touchBarrierSnapshot(resources, state.sessionId, newOutstanding, held.size)
            result <- AgentProcessing.pipeLlmCall(
              agentDef,
              resources,
              depth,
              parentRef,
              state
                .withMessages(
                  state.messages :+ Message(
                    MessageRole.User,
                    Left(injectionText),
                    source = visSource.orElse(Some("external"))
                  )
                )
                .withOutstandingSubagentResults(newOutstanding)
                .withNextLoopTurn, // Block 3：外部事件唤醒 = 新 turn
              None
            )
          yield result
          end for
        else if isSubagentResult && held.nonEmpty then
          // Batch complete: inject ALL held results (plus this one) together.
          val batchMessage = buildEventReminder(held :+ event)
          for
            _ <- sessionBusyIO
            _ <- receiveVisibility(waiting = false)
            _ <- touchBarrierSnapshot(resources, state.sessionId, 0, 0)
            result <- AgentProcessing.pipeLlmCall(
              agentDef,
              resources,
              depth,
              parentRef,
              state
                // 2026-09-27 裁定（ORCH1-R8）：批完成清队（outstanding 归零 + HELD
                // 队列清空）改指 TurnBoundary.clearHeldEvents，语义逐字不变。
                .copy(execution = TurnBoundary.clearHeldEvents(state.execution))
                .withMessages(state.messages :+ batchMessage)
                .withNextLoopTurn, // Block 3：批次汇聚唤醒 = 新 turn
              None
            )
          yield result
          end for
        else
          // Single immediate result (no batch in flight) or non-subagent event.
          // ═══ 节点完成闸（bgtask-completion-gate 批，作者 2026-09-05 18:29 裁定）═══
          // ExternalEvent 唤醒轮此前 replyTo=None → 续跑轮 AgentEvent.Completed
          // 无人接收——节点观察桥（完成闸单咽喉）在最后一个后台任务完成通知后
          // 永远等不到复检事件，节点悬挂到兜底上限。修正：node- 前缀（Project
          // 节点，kind=Flow）会话以 AgentRecord.supervisorRef（=NodeEngine 观察
          // 桥，注册即粘性完成目标）作为本唤醒轮的 replyTo。仅 node- 节点启用：
          // delegate/subtask（BackoffSupervisor 会把 Completed 当任务终态转投父
          // 会话=重复通知+停 actor）与 dispatcher 桥（pendingInjected 计数契约，
          // 意外 Completed 会提前拆除）各有既有 Completed 语义，不并入。
          val continuationReplyTo: IO[Option[ActorRef[AgentEvent]]] =
            state.sessionId
              .filter(sid => state.isFlowNode && sid.startsWith(nebflow.core.project.NodeEngine.SessionPrefix))
              .fold(IO.pure(Option.empty[ActorRef[AgentEvent]]))(sid =>
                resources.agentRegistry.get.map(_.get(sid).flatMap(_.supervisorRef))
              )
          continuationReplyTo.flatMap { contReplyTo =>
            for
              _ <- sessionBusyIO
              _ <- receiveVisibility(waiting = false)
              _ <- touchBarrierSnapshot(resources, state.sessionId, newOutstanding, 0)
              result <- AgentProcessing.pipeLlmCall(
                agentDef,
                resources,
                depth,
                parentRef,
                state
                  .withMessages(
                    // Reminder refactor (2026-08-20): always carry a source marker —
                    // external-event turns are system events, not real user input
                    // (fromUser = Message.source.isEmpty).
                    state.messages :+ Message(
                      MessageRole.User,
                      Left(injectionText),
                      source = visSource.orElse(Some("external"))
                    )
                  )
                  // #25: apply the barrier decrement. Pre-fix this branch
                  // computed newOutstanding and dropped it — a single-result
                  // batch (spawn 1, receive 1, held empty) left the counter
                  // stuck at 1 forever. Invisible while Completed fired
                  // unconditionally at turn end; once the completion debt is
                  // parked (#25) a stuck counter means the debt is NEVER paid
                  // and the waiting supervisor/bridge hangs.
                  .withOutstandingSubagentResults(newOutstanding)
                  .withNextLoopTurn, // Block 3：外部事件唤醒 = 新 turn
                contReplyTo
              )
            yield result
            end for
          }
        end if

      case AgentCommand.UpdateGitBranch(branch) =>
        updateGitBranchStay(state, branch)(s => IO.pure(idle(agentDef, resources, depth, parentRef, s)))

      case AgentCommand.CompactionComplete(result) =>
        // 2026-09-27 裁定（ORCH2-P1）：stale 兜底公共段与 AgentFrozen 逐字同形 ⇒
        // 改指 BehaviorCommon.staleCompactionDiscard；deferred 结算文案字面量原样
        // 留案体作实参，本态 Some 腿**不**清 pendingCompaction（与 frozen 之
        // 设计差）以 clearPendingCompaction=false 显式参数化，回本态构造以 stay
        // 续参注入（原「Compaction finished…」注释随迁共享方法体）。
        BehaviorCommon.staleCompactionDiscard(
          agentDef,
          depth,
          state,
          result,
          "Compaction result arrived after agent returned to idle",
          clearPendingCompaction = false
        )(s => IO.pure(idle(agentDef, resources, depth, parentRef, s)))

      // 2026-09-13（permshield S1）：`AgentCommand.SetSafetyMode` 已退役（档位 =
      // 应用级持久值，WS/REST 写入口直接落盘 + 热读，无需通知活 agent 改副本）；
      // 由下方 catch-all 兜底为状态不变。三处（idle/processing/frozen）同。

      case _: AgentCommand.LlmComplete | _: AgentCommand.LlmFailed | _: AgentCommand.ToolsComplete |
          _: AgentCommand.SetPermissionDeferred =>
        IO.pure(idle(agentDef, resources, depth, parentRef, state))

      // F2 (2026-08-30): replay persisted injection queues after a crash.
      // Idempotent: merges disk entries ahead of the in-memory queues (which
      // are empty at spawn); the disk file is deleted by the drain paths once
      // the queues are flushed, so a second replay is a no-op.
      case AgentCommand.RecoverPersistedQueues =>
        val sid = state.sessionId.getOrElse("")
        CompactionQueueStore.load(sid).flatMap {
          case Some(q) if q.imms.nonEmpty || q.events.nonEmpty =>
            logAgentEvent(
              agentDef,
              depth,
              state.sessionId,
              state.sessionName,
              "queues-recovered",
              s"imm=${q.imms.size} events=${q.events.size}"
            )
            // 2026-09-27 裁定（ORCH1-R8）：崩溃恢复预插（磁盘条目排在内存队列
            // 之前）改指 TurnBoundary.recoverPersistedQueues，语义逐字不变。
            val exec = TurnBoundary.recoverPersistedQueues(state.execution, q.imms, q.events)
            IO.pure(idle(agentDef, resources, depth, parentRef, state.copy(execution = exec)))
          case _ => IO.pure(idle(agentDef, resources, depth, parentRef, state))
        }

      // Immediate input arriving in idle (turn already finished) — treat as normal UserInput
      // ② (2026-09-11): `fromUser` is carried across the conversion — dropping it
      // here is exactly what made a real human text land in the
      // `clientMessageId=None ⇒ source="tool"` fallback (diagnosis §1.4 idle row).
      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 ImmediateInput 转 UserInput
      // 直投，processing/frozen 为排队腿——真语义差异 ⇒ 整站留。
      case AgentCommand.ImmediateInput(
            text,
            blocks,
            source,
            eventType,
            sender,
            senderTeam,
            delivery,
            fromUser,
            project
          ) =>
        for _ <- ctx.self ! AgentCommand.UserInput(
            text,
            None,
            None,
            blocks,
            0,
            source,
            sender,
            senderTeam,
            delivery,
            eventType,
            None,
            fromUser,
            project
          )
        yield idle(agentDef, resources, depth, parentRef, state)

      // Queued mail arriving in idle — drain immediately as a new turn.
      // Idempotent guard (#22): activation sends a head-trigger MailQueued AND
      // the delivering path sends its own for the same item — without the
      // head-check the duplicate injected the same task as TWO turns (double
      // LLM calls, double tool work). Only drain when this item is still the
      // disk head; stale/duplicate triggers are no-ops.
      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 MailQueued = 空闲 gate
      // 全量投递（#407），与 processing/frozen 的计数腿（本批已收口
      // BehaviorCommon.mailQueuedCountUp）真语义差异 ⇒ 整站留。
      case AgentCommand.MailQueued(item, _) =>
        val sid = state.sessionId.getOrElse("")
        for
          items <- nebflow.core.flow.MailQueueStore.load(sid)
          headOpt = items.headOption
          // #407 idle gate（08-28 统一裁定）：目标 agent 自身+子树完全空闲才
          // 投递；不空闲则延迟（不消费队列，pendingMailQueueCount=1 保持「有货
          // 未投递」状态），由子 agent 完成事件驱动的 turn 结束重检
          // （finishTurnCont drain 分支的 gate）投递。
          idleOk <- AgentFinishTurn.fullyIdle(sid, state, resources)
          result <-
            if !idleOk then
              IO(
                logAgentEvent(
                  agentDef,
                  depth,
                  state.sessionId,
                  state.sessionName,
                  "mail-queued-deferred",
                  s"head=${headOpt.map(_.id).getOrElse("-")} sender=${headOpt.map(_.fromSession).getOrElse("-")} subtree-busy, deferred"
                )
              ) *>
                IO.pure(
                  idle(
                    agentDef,
                    resources,
                    depth,
                    parentRef,
                    // #10 (2026-08-27 mail-queue wedge): accumulate, don't clamp —
                    // multiple mails arriving while idle+subtree-busy each take
                    // this deferral path; `= 1` would lose the earlier count and
                    // the turn-end drain would deliver only the last one.
                    state.copy(execution =
                      state.execution.copy(pendingMailQueueCount = state.execution.pendingMailQueueCount + 1)
                    )
                  )
                )
            else
              headOpt match
                case Some(head) if head.id == item.id =>
                  // Delivery-layer fingerprint dedup (P0): suppresses the same
                  // sender+recipient+content within the 30min window — covers the
                  // restart-replay root cause (MailTool restart recovery re-fires
                  // MailQueued for the disk head). The persisted fingerprint file
                  // survives the restart boundary.
                  // V13 (2026-09-03): the consult is scoped to the replay window
                  // after this session's activation — outside it a same-content
                  // re-send is legitimate and delivers unconditionally.
                  mailDedupInReplayWindow(sid, resources)
                    .flatMap { inReplayWindow =>
                      nebflow.core.flow.MailDeliveryDedup
                        .tryDeliver(
                          sid,
                          nebflow.core.flow.MailDeliveryDedup.fingerprint(item.from, sid, item.message),
                          System.currentTimeMillis(),
                          inReplayWindow
                        )
                        .flatMap {
                          case false =>
                            // Duplicate within window: consume the item (queue + WS)
                            // but inject nothing — sender semantics untouched.
                            nebflow.core.flow.MailQueueStore.removeHead(sid).void *>
                              AgentFinishTurn.emitDequeuedWs(state.wsSend, sid, item.id)
                          case true =>
                            for
                              _ <- nebflow.core.flow.MailQueueStore.removeHead(sid).void
                              // G3: re-read attachment paths at drain time (D6 — queue
                              // persists paths, not base64). Lost files degrade to
                              // placeholder text.
                              attBlocks <- nebflow.core.tools.ImageInject.drainImagePaths(item.imagePaths)
                              blocks = nebflow.core.tools.ImageInject.messageBlocks(item.message, attBlocks)
                              _ <- ctx.self ! AgentCommand.UserInput(
                                item.message,
                                None,
                                None,
                                blocks,
                                0,
                                source = Some("mail-queue"),
                                sender = Some(item.from),
                                delivery = Some("queue")
                              )
                              // Emit WS so frontend removes the pending item
                              _ <- AgentFinishTurn.emitDequeuedWs(state.wsSend, sid, item.id)
                            yield ()
                        }
                    }
                    .map(_ => idle(agentDef, resources, depth, parentRef, state))
                case _ => IO.pure(idle(agentDef, resources, depth, parentRef, state))
        yield result
        end for

      // Supervisor restart in idle state
      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 RestartAgent 与
      // processing/frozen 收敛面（BehaviorCommon.restartAgentCore）不同形——本态
      // 无 cancelCurrentTurn / 压缩轮 deferred 结算 / Interrupted 帧三腿（无在飞
      // turn）⇒ 原样保留（log 字面量 "restart-idle" 亦为本站专属）。
      case AgentCommand.RestartAgent(level) =>
        logAgentEvent(agentDef, depth, state.sessionId, state.sessionName, "restart-idle", s"level=${level.toString}")
        for
          restartState <- AgentProcessing.restartStateFor(level, state, resources)
          _ <- state
            .wsSend(
              Json.obj(
                "type" -> "agentRestarted".asJson,
                "sessionId" -> state.sessionId.asJson,
                "level" -> level.toString.asJson
              )
            )
            .handleErrorWith(_ => IO.unit)
          result <- AgentProcessing.pipeLlmCall(
            agentDef,
            resources,
            depth,
            parentRef,
            restartState.withNextLoopTurn,
            None
          )
        yield result
        end for

      case _ =>
        IO.pure(idle(agentDef, resources, depth, parentRef, state))
  end idle

end AgentIdle
