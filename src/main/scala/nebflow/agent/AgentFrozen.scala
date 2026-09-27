/* 从 AgentActor 迁出(行为保持重构,2026-09-25)。 */
package nebflow.agent

import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Json, JsonObject}
import nebflow.actor.*
import nebflow.core.*
import nebflow.core.ask.AskService
import nebflow.shared.{NebflowLogger, *}

/**
 * 冻结域:原 object AgentActor 内 enterErrorFrozen / enterFrozen / frozen 的实现
 * 整体迁入本件(方法体逐字未动;frozen 行为内部的 actor 变换与自递归——返回下一
 * frozen behavior——保持原逻辑)。AgentActor 侧保留同名委托 def(签名与默认参数
 * 原样),全部调用点零改动;notifyEscalation / sessionIdOfRef /
 * updateRegistryEscalation / updateRegistryFrozenReason / currentNextChange /
 * reasonStr / ErrorFreezeEscalationThreshold 等冻结域 helper 与常量留守
 * AgentActor,经 import AgentActor.* 引用。
 */
private[agent] object AgentFrozen:
  import nebflow.agent.AgentActor.*

  /**
   * v2 错误冻结转入辅助（§3.3/§6.1 步骤 3）：transient 类 LLM 失败 → 立即
   * persist（复用 fatal 路径的 save 模式）→ enterErrorFrozen——冻结期间零 LLM
   * 调用（D2），退避到期 CheckFreezeGate 续跑（重新过 gate，天然闭环）。
   */
  private[agent] def enterErrorFrozen(
    agentDef: AgentDef,
    resources: SharedResources,
    depth: Int,
    parentRef: Option[ActorRef[AgentCommand]],
    state: AgentState,
    replyTo: Option[ActorRef[AgentEvent]],
    reason: FreezeReason,
    resumeInMs: Long,
    error: Throwable
  )(using ctx: ActorContext[AgentCommand]): IO[Behavior[AgentCommand]] =
    for
      _ <- state.sessionId.fold(IO.unit) { sid =>
        ctx.forkTurn(
          (resources.sessionStore.saveMessagesForSession(sid, state.messages) *>
            resources.sessionStore.flushIndex)
            .handleErrorWith(e => NebflowLogger.forName("nebflow.agent").warn(s"Save failed session: ${e.getMessage}"))
        )
      }
      _ <- IO {
        logAgentEvent(
          agentDef,
          depth,
          state.sessionId,
          state.sessionName,
          "error-freeze",
          s"reason=${reasonStr(reason)} err=${Option(error.getMessage).getOrElse("unknown").take(80)} resumeIn=${resumeInMs}ms"
        )
      }
      result <- enterFrozen(
        agentDef,
        resources,
        depth,
        parentRef,
        state,
        replyTo,
        Some(System.currentTimeMillis() + resumeInMs),
        reason,
        Some(Option(error.getMessage).getOrElse("unknown").take(200))
      )
    yield result

  /**
   * gate 拦截后的转入辅助：Frozen 事件 + registry 标记 + 进入 frozen behavior。
   * v2（冻结式错误恢复）：reason 泛化——Schedule=时间表（默认，旧调用点零改动）；
   * 错误族（LlmTransient/Network/ProviderDown/RestartRecovery）附加连续计数与
   * 升级链判定（同 reason 连续 ≥3 → 进入升级链，通知父/用户，不再直接 fatal）。
   */
  private[agent] def enterFrozen(
    agentDef: AgentDef,
    resources: SharedResources,
    depth: Int,
    parentRef: Option[ActorRef[AgentCommand]],
    state: AgentState,
    replyTo: Option[ActorRef[AgentEvent]],
    resumeAt: Option[Long],
    reason: FreezeReason = FreezeReason.Schedule,
    detail: Option[String] = None
  )(using ctx: ActorContext[AgentCommand]): IO[Behavior[AgentCommand]] =
    val isErrorFamily = reason != FreezeReason.Schedule
    // 连续计数：Schedule 不参与；错误族 reason 相同则 +1（跨恢复累计），变化重置为 1
    val count =
      if isErrorFamily then if state.lastErrorFreezeReason.contains(reason) then state.errorFreezeCount + 1 else 1
      else 0
    // 升级链进入判定（§5.1）：连续 ≥3 且尚未在升级链中（幂等——不重复通知）
    val (escalation, escalateNotify): (Option[EscalationInfo], IO[Unit]) =
      if isErrorFamily && count >= ErrorFreezeEscalationThreshold && state.escalation.isEmpty then
        val now = System.currentTimeMillis()
        val esc = EscalationInfo(
          level = 1,
          escalateAt = now + nebflow.shared.Defaults.ErrorEscalateAfterMs,
          awaitedParentSessionId = ""
        )
        (Some(esc), notifyEscalation(agentDef, depth, state, parentRef, reason, count, esc))
      else (state.escalation, IO.unit)
    escalateNotify *>
      emitStream(
        state.wsSend,
        AgentStreamEvent.Frozen(resumeAt, reason, detail, count, escalation),
        isSubagent = depth > 0,
        state.sessionId
      ) *>
      touchRegistryActivity(resources, state.sessionId, AgentStatus.Frozen) *>
      updateRegistryEscalation(resources, state.sessionId, escalation) *>
      updateRegistryFrozenReason(resources, state.sessionId, Some(reasonStr(reason))) *>
      // 计数/escalation 写回 state（frozen 闭包参数不持久——LlmFailed 从 state
      // 读 errorFreezeCount 判定「同 reason 连续」；不写回则每轮都从 0 计数，
      // 升级链永不触发）。Schedule 冻结不参与计数（原样保留 state）。
      IO.pure {
        val frozenState =
          if isErrorFamily then state.withErrorFreezeCount(count, reason).withEscalation(escalation)
          else state
        frozen(agentDef, resources, depth, parentRef, frozenState, replyTo, resumeAt, reason, count, escalation)
      }

  end enterFrozen

  /**
   * frozen behavior：冻结中的 agent——持有完整 state（工具结果已组装并持久化，
   * F1）+ replyTo，等待出冻结段自动恢复或用户唤醒。
   *
   * 恢复驱动只有两个：CheckFreezeGate（FreezeScheduler 30s 轮询 / 配置热更即时
   * scan）重评估时间表；UserInput(clientMessageId.isDefined) 用户唤醒。系统注入
   * 一律排队不唤醒（零 token 铁律，D2）——BackoffSupervisor 崩溃重启注入的
   * "continue" 在冻结时段同样不唤醒，出冻结段后续跑（spec §5.3）。
   *
   * 独立 behavior 而非 processing 加 flag（D3）：天然隔离 processing 的 20+ 交互
   * case（重复 ToolsComplete 双 dispatch、stale LlmComplete 等）；catch-all 留在
   * frozen 态——stale 消息丢弃，但冻结前已持久化的状态不丢。
   */
  private[agent] def frozen(
    agentDef: AgentDef,
    resources: SharedResources,
    depth: Int,
    parentRef: Option[ActorRef[AgentCommand]],
    state: AgentState,
    replyTo: Option[ActorRef[AgentEvent]],
    resumeAt: Option[Long],
    reason: FreezeReason = FreezeReason.Schedule,
    retryCount: Int = 0,
    escalation: Option[EscalationInfo] = None
  )(using ctx: ActorContext[AgentCommand]): Behavior[AgentCommand] =
    Behaviors.receiveMessage:

      case AgentCommand.CheckFreezeGate =>
        // 唯一自动恢复驱动：重评估（支持运行中改配置/时钟漂移）。仍冻结 →
        // 原地留任（静默更新 resumeAt——不重发 Frozen 事件，避免 30s 轮询刷屏）；
        // 冻结时段已过（段外=工作时段）→ Resumed + 恢复挂起的 dispatch
        // （cause=Gated，但已不在冻结段 → 通过）。
        val now = System.currentTimeMillis()
        reason match
          case FreezeReason.Schedule =>
            // 时间表冻结：现有 eval 语义（#337 黑名单——段外=工作时段）+ 2026-08-25
            // 用户消息全局跳过（skipUntil 未到期 → 视为段外，立即恢复）。
            for
              cfg <- resources.freezeScheduleRef.get
              skipUntil <- resources.freezeSkipUntilRef.get
              window = nebflow.core.schedule.FreezeSchedule.evalWithSkip(cfg, skipUntil, now)
              result <-
                if !window.frozen then
                  logAgentEvent(
                    agentDef,
                    depth,
                    state.sessionId,
                    state.sessionName,
                    "freeze-resume",
                    "reason=outside-freeze-segment"
                  )
                  emitStream(
                    state.wsSend,
                    AgentStreamEvent.Resumed(window.nextChangeAt),
                    isSubagent = depth > 0,
                    state.sessionId
                  ) *> updateRegistryFrozenReason(resources, state.sessionId, None) *>
                    pipeLlmCall(agentDef, resources, depth, parentRef, state, replyTo)
                else
                  IO.pure(
                    frozen(
                      agentDef,
                      resources,
                      depth,
                      parentRef,
                      state,
                      replyTo,
                      window.nextChangeAt,
                      reason,
                      retryCount,
                      escalation
                    )
                  )
            yield result
          case _ =>
            // 错误族（LlmTransient/Network/ProviderDown/RestartRecovery）：退避到期
            // （resumeAt = 进入时计算的退避时刻）→ 续跑——重新过 gate（pipeLlmCall），
            // 条件未消除再入冻结，天然闭环；ProviderDown 的候选 health 检查 P0 用
            // 退避到期 + 续跑重试兜底（R1 轮询），P1 接 HealthMonitor markUp 事件。
            val shouldResume = resumeAt.forall(now >= _)
            if shouldResume then
              logAgentEvent(
                agentDef,
                depth,
                state.sessionId,
                state.sessionName,
                "freeze-resume",
                s"reason=${reasonStr(reason)} backoff-elapsed"
              )
              currentNextChange(resources).flatMap { nextChange =>
                emitStream(
                  state.wsSend,
                  AgentStreamEvent.Resumed(nextChange),
                  isSubagent = depth > 0,
                  state.sessionId
                ) *> updateRegistryFrozenReason(resources, state.sessionId, None) *>
                  pipeLlmCall(agentDef, resources, depth, parentRef, state, replyTo)
              }
            else
              IO.pure(
                frozen(agentDef, resources, depth, parentRef, state, replyTo, resumeAt, reason, retryCount, escalation)
              )
            end if
        end match

      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 UserInput = 用户唤醒
      // （clientMessageId 守卫分叉：唤醒 Resumed 链 vs 排队不唤醒），与 idle
      // 直投开轮真语义差异 ⇒ 整站留（相同碎片量过小，不提取）。
      case AgentCommand.UserInput(
            text,
            replyTo2,
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
        if clientMessageId.isDefined then
          // ★ 用户唤醒（B5）：注入用户消息到冻结中的上下文，立即 dispatch——
          // 冻结前组装好的工具结果 + 用户新指令同轮喂给 LLM。dedup 防止 WS
          // 重发导致同一条用户消息注入两次（v1.1）。
          val (isDuplicate, dedupedState) = checkDuplicate(clientMessageId, state)
          if isDuplicate then
            logger.info(s"[frozen] Dropping duplicate wake message clientMessageId=${clientMessageId.getOrElse("")}")
            IO.pure(
              frozen(agentDef, resources, depth, parentRef, state, replyTo, resumeAt, reason, retryCount, escalation)
            )
          else
            val userMsg = blocks.filter(_.nonEmpty) match
              case Some(bl) => Message(MessageRole.User, Right(bl))
              case None => Message(MessageRole.User, Left(text))
            val stateWithWidth =
              if chatWidth > 0 then dedupedState.copy(session = dedupedState.session.copy(chatWidth = chatWidth))
              else dedupedState
            val wakeState = stateWithWidth
              .withMessages(stateWithWidth.messages :+ userMsg)
              .withEmptyResponseRetries(0)
              .withMailUsedThisTurn(false)
              .withNextLoopTurn // Block 3：冻结唤醒 = 新 turn
              // R1（2026-09-10 冻结族最小修法）：用户唤醒 = 新 turn 纪元 **且**
              // 重新开始 LoopGuard 跨 turn 观察窗——不重置则唤醒前累积的同 fp
              // 失败记录把唤醒后的第 1 次失败直接推过 crossTurnFailureTurns
              // （实测唤醒后 6.65s / 21.26s 复冻）。terminatedFps 不在此列：
              // 被 L1 终止过的 fp 复发仍即刻 Freeze（既有语义，冻结不受影响）。
              .withLoopCounters(stateWithWidth.loopCounters.resetCrossTurn)
            logAgentEvent(agentDef, depth, state.sessionId, state.sessionName, "freeze-wake", s"text=${text.take(60)}")
            currentNextChange(resources).flatMap { nextChange =>
              emitStream(
                state.wsSend,
                AgentStreamEvent.Resumed(nextChange),
                isSubagent = depth > 0,
                state.sessionId
              ) *> updateRegistryFrozenReason(resources, state.sessionId, None) *>
                pipeLlmCall(agentDef, resources, depth, parentRef, wakeState, replyTo2, DispatchCause.UserWake)
            }
          end if
        else
          // 系统注入（Mail/Delegate/BackoffSupervisor continue）：排队不唤醒——
          // 恢复后的 turn 结束时由 finishTurnCont drain（head 重发，idle 全量处理）。
          logAgentEvent(
            agentDef,
            depth,
            state.sessionId,
            state.sessionName,
            "freeze-queue-input",
            s"textLen=${text.length}"
          )
          // 2026-09-27 裁定（ORCH1-R8）：入队改指 TurnBoundary.enqueueUserInput
          // （追加语义 :+ 逐字不变；UserInput 重建 12 参原样——本腿不转 project
          // 字段是既有行为，逐字保持）。
          val queued = state.copy(execution =
            TurnBoundary.enqueueUserInput(
              state.execution,
              AgentCommand.UserInput(
                text,
                replyTo2,
                clientMessageId,
                blocks,
                chatWidth,
                source,
                sender,
                senderTeam,
                delivery,
                eventType,
                intake,
                fromUser
              )
            )
          )
          IO.pure(
            frozen(agentDef, resources, depth, parentRef, queued, replyTo, resumeAt, reason, retryCount, escalation)
          )

      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 AskQuestion = 冻结唤醒
      // （Resumed 帧 + frozenReason 清位 + resetCrossTurn 观察窗重启），idle 态
      // 无这三腿——真语义差异 ⇒ 整站留。
      case AgentCommand.AskQuestion(question, _) =>
        // 用户动作（D2）：唤醒——ask 轮本身也是 gate 豁免路径（askMode.isDefined）。
        logAgentEvent(agentDef, depth, state.sessionId, state.sessionName, "freeze-wake", s"ask=${question.take(60)}")
        val askReminder = AskService.buildAskReminder(question)
        val askState = state
          .withMessages(state.messages :+ askReminder)
          .withAskMode(Some(question))
          .withStatus(AgentStatus.Processing)
          .withNextLoopTurn // Block 3：冻结唤醒 = 新 turn
          // A 轨（2026-09-10 冻结族最小修法，形态照 R1）：/ask 唤醒同样是新 turn
          // 纪元 **且** 重新开始 LoopGuard 跨 turn 观察窗——不重置则唤醒前累积的
          // 同 fp 失败记录把唤醒后的第 1 次失败直接推过 crossTurnFailureTurns
          // （LoopGuardWiringSpec 先红实证：唤醒轮即 "failed in 4 separate turns"
          // 秒冻）。terminatedFps 不在此列：被 L1 终止过的 fp 复发仍即刻 Freeze
          // （既有语义，冻结不受影响）。
          .withLoopCounters(state.loopCounters.resetCrossTurn)
        currentNextChange(resources).flatMap { nextChange =>
          emitStream(
            state.wsSend,
            AgentStreamEvent.Resumed(nextChange),
            isSubagent = depth > 0,
            state.sessionId
          ) *> updateRegistryFrozenReason(resources, state.sessionId, None) *>
            pipeLlmCall(agentDef, resources, depth, parentRef, askState, None, DispatchCause.UserWake)
        }

      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 SkillActivate = 冻结唤醒
      // （Resumed 链 + resetCrossTurn），idle 态为 sessionBusyIO 直投——发射腿
      // 已 ORCH1-R1 归口 TurnBoundary.emitForSkill，剩余量过小 ⇒ 整站留。
      case AgentCommand.SkillActivate(skillName, input, _, skillContent, _) =>
        // 用户动作（D2）：唤醒（镜像 idle 的 SkillActivate handler）。
        logAgentEvent(agentDef, depth, state.sessionId, state.sessionName, "freeze-wake", s"skill=$skillName")
        val combinedText = s"<skill name=\"$skillName\">\n$skillContent\n</skill>\n\n$input"
        val processingState = state
          .withMessages(
            state.messages :+ Message(MessageRole.User, Left(combinedText), source = Some("skill"))
          )
          .withStatus(AgentStatus.Processing)
          .withNextLoopTurn // Block 3：冻结唤醒 = 新 turn
          // A 轨（2026-09-10 冻结族最小修法，形态照 R1）：skill 激活唤醒同样是新
          // turn 纪元 **且** 重新开始 LoopGuard 跨 turn 观察窗——不重置则唤醒前
          // 累积的同 fp 失败记录把唤醒后的第 1 次失败直接推过
          // crossTurnFailureTurns（LoopGuardWiringSpec 先红实证：唤醒轮即
          // "failed in 4 separate turns" 秒冻）。terminatedFps 不在此列（L1 终止过
          // 的 fp 复发仍即刻 Freeze，既有语义不变）。
          .withLoopCounters(state.loopCounters.resetCrossTurn)
        for
          _ <- currentNextChange(resources).flatMap { nextChange =>
            emitStream(
              state.wsSend,
              AgentStreamEvent.Resumed(nextChange),
              isSubagent = depth > 0,
              state.sessionId
            ) *> updateRegistryFrozenReason(resources, state.sessionId, None)
          }
          // 2026-09-27 裁定（ORCH1-R1 P1 对1）：与 AgentIdle SkillActivate 唤醒腿
          // 逐字同形 ⇒ 统一改指 TurnBoundary.emitForSkill。
          _ <- TurnBoundary.emitForSkill(resources, state, input)
          result <- pipeLlmCall(agentDef, resources, depth, parentRef, processingState, None, DispatchCause.UserWake)
        yield result
        end for

      case AgentCommand.Interrupt() =>
        // D10 放弃续跑：与 processing 的 Interrupt 同语义（cancelCurrentTurn +
        // Interrupted 事件 + idle，历史含工具结果保留）。
        // 2026-09-27 裁定（ORCH2-P1）：公共段与 AgentProcessing Interrupt 逐字同形 ⇒
        // 改指 BehaviorCommon.interruptToIdle——compactui/frozen 专属 registry 退回
        // Idle 等随迁注释逐字并置共享方法体（一字不改、不删、不合并），log detail 字面量
        // 留案体，本站尾腿（registry frozenReason 清位）以 afterRegistry 实参
        // 显式参数化。
        logAgentEvent(agentDef, depth, state.sessionId, state.sessionName, "interrupt", "reason=user-during-frozen")
        BehaviorCommon.interruptToIdle(agentDef, resources, depth, parentRef, state)(
          updateRegistryFrozenReason(resources, state.sessionId, None)
        )

      case AgentCommand.Stop(_) =>
        // 2026-09-27 裁定（ORCH2-P1）：三态 Stop 公共序列收口
        // BehaviorCommon.stopSequence；本态两枚 registry 腿（退回 Idle——否则
        // FreezeScheduler 持续 ping——并清 frozenReason）作为 betweenKillAndHooks
        // 实参显式参数化，log detail 字面量留案体，语义逐字不变。
        logAgentEvent(agentDef, depth, state.sessionId, state.sessionName, "stop", "reason=user-during-frozen")
        BehaviorCommon.stopSequence(resources, state)(
          touchRegistryActivity(resources, state.sessionId, AgentStatus.Idle) *>
            updateRegistryFrozenReason(resources, state.sessionId, None)
        )

      case AgentCommand.RestartAgent(level) =>
        // 镜像 processing 的 RestartAgent；末尾 dispatch 是系统动作（Gated）——
        // 冻结时段再次进入 frozen（正确语义：supervisor 重启不唤醒）。
        // 2026-09-27 裁定（ORCH2-P1 · P2 升级裁定）：公共段与 AgentProcessing RestartAgent 逐字
        // 同形 ⇒ 改指 BehaviorCommon.restartAgentCore；本态 dispatchState =
        // identity（**不**刷新 loop 纪元——本注释所述冻结语义的一部分，与
        // processing 之 _.withNextLoopTurn 相对，差异显式参数化）。
        logAgentEvent(agentDef, depth, state.sessionId, state.sessionName, "restart", s"level=${level.toString}")
        BehaviorCommon.restartAgentCore(agentDef, resources, depth, parentRef, state, level)(identity)

      case AgentCommand.Retry(reason) =>
        // 镜像 processing 的 Retry：从 checkpoint 重派——Gated，冻结时段再次冻结
        // （不烧 token，spec §5.4）。F（2026-09-25 命令消重）：两态逐字同形,
        // 收敛至 AgentActor.retryFromCheckpoint。
        retryFromCheckpoint(agentDef, resources, depth, parentRef, state, reason)

      case AgentCommand.Escalate =>
        // v2 升级链到期（§5.2）：FreezeScheduler.scan 发现 escalation.escalateAt
        // 超时 → level+1，通知上一级；父缺失 → 跳级（P0=到达用户终态）；无父 →
        // 用户终态（WS errorEscalated，不设超时——用户是最终仲裁）。
        val current = escalation.getOrElse(EscalationInfo(level = 1, escalateAt = 0L, awaitedParentSessionId = ""))
        val nextLevel = current.level + 1
        val now = System.currentTimeMillis()
        for
          parentSid <- sessionIdOfRef(resources, parentRef)
          target = Escalation.nextTarget(nextLevel, parentRef.isDefined, parentSid.isDefined)
          newEsc =
            if target == Escalation.Target.User then current.copy(level = nextLevel, escalateAt = 0L)
            else current.copy(level = nextLevel, escalateAt = now + nebflow.shared.Defaults.ErrorEscalateAfterMs)
          _ <- IO {
            logAgentEvent(
              agentDef,
              depth,
              state.sessionId,
              state.sessionName,
              "error-escalate",
              s"level=$nextLevel target=$target retryCount=$retryCount"
            )
          }
          _ <- target match
            case Escalation.Target.Parent =>
              parentRef.traverse_ { p =>
                p ! AgentCommand.ExternalEvent(
                  source = "team",
                  eventType = "error-escalated",
                  payload =
                    s""""${agentDef.name}" auto-recovery escalation level $nextLevel (reason=${reasonStr(reason)})""",
                  metadata = JsonObject(
                    "reason" -> reasonStr(reason).asJson,
                    "retryCount" -> retryCount.asJson,
                    "level" -> nextLevel.asJson,
                    "escalateAfterMs" -> nebflow.shared.Defaults.ErrorEscalateAfterMs.asJson,
                    "failedSessionId" -> state.sessionId.getOrElse("").asJson,
                    "agentName" -> agentDef.name.asJson
                  ),
                  correlationId = state.sessionId.filter(_.nonEmpty)
                )
              }
            case Escalation.Target.Grandparent =>
              // 父缺失跳级（P0）：视同到达用户终态（无再上级信息），WS 通知 + 不设超时
              state
                .wsSend(
                  Json.obj(
                    "type" -> "errorEscalated".asJson,
                    "sessionId" -> state.sessionId.getOrElse("").asJson,
                    "reason" -> reasonStr(reason).asJson,
                    "retryCount" -> retryCount.asJson,
                    "level" -> nextLevel.asJson,
                    "escalateAt" -> 0L.asJson,
                    "detail" -> "parent-unavailable-skipped".asJson
                  )
                )
                .handleErrorWith(_ => IO.unit)
            case Escalation.Target.User =>
              state
                .wsSend(
                  Json.obj(
                    "type" -> "errorEscalated".asJson,
                    "sessionId" -> state.sessionId.getOrElse("").asJson,
                    "reason" -> reasonStr(reason).asJson,
                    "retryCount" -> retryCount.asJson,
                    "level" -> nextLevel.asJson,
                    "escalateAt" -> 0L.asJson
                  )
                )
                .handleErrorWith(_ => IO.unit)
          _ <- updateRegistryEscalation(resources, state.sessionId, Some(newEsc))
        yield
          val escalatedState = state.withEscalation(Some(newEsc))
          frozen(
            agentDef,
            resources,
            depth,
            parentRef,
            escalatedState,
            replyTo,
            resumeAt,
            reason,
            retryCount,
            Some(newEsc)
          )
        end for

      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态 ExternalEvent = 静默排队
      // （零 token 铁律，不出气泡不 persist），processing 态带收件气泡 +
      // persistQueues、idle 态 barrier 注入开轮——真语义差异 ⇒ 整站留；入队 +
      // barrier 递减已 ORCH1-R8 单点。
      case AgentCommand.ExternalEvent(source, eventType, payload, metadata, correlationId) =>
        // 排队 pendingEvents（不唤醒）：出冻结段/唤醒后的下一个 turn 边界
        // （ToolsComplete → drainBarrier）统一注入。barrier 计数语义镜像 idle
        // ExternalEvent——subagent result 到达即递减 outstanding，否则批次在
        // 冻结期间全部完成时计数永不清零，恢复后 drainBarrier 永久 hold。
        logAgentEvent(
          agentDef,
          depth,
          state.sessionId,
          state.sessionName,
          "freeze-queue-event",
          s"source=$source type=$eventType"
        )
        val event = AgentCommand.ExternalEvent(source, eventType, payload, metadata, correlationId)
        // 2026-09-27 裁定（ORCH1-R8）：pendingEvents 入队 + barrier 递减改指
        // TurnBoundary.enqueueExternalEvent（与 AgentProcessing 递减腿逐字同形 ⇒
        // 单点；上方 barrier 语义注释原样留守本站）。
        val queued = state.copy(execution = TurnBoundary.enqueueExternalEvent(state.execution, event))
        IO.pure(
          frozen(agentDef, resources, depth, parentRef, queued, replyTo, resumeAt, reason, retryCount, escalation)
        )

      // 2026-09-27 裁定（ORCH2-P3 保留:语义差异）：本态排队腿与 processing 同形
      // 除 persistQueues（F2 快照纪律，与 processing 侧注记配对）——真语义差异
      // ⇒ 整站留（入队面已 ORCH1-R8 单点）。
      case msg: AgentCommand.ImmediateInput =>
        // 排队 pendingImmediateInputs（不唤醒）——恢复后的 turn 边界 drain。
        logAgentEvent(
          agentDef,
          depth,
          state.sessionId,
          state.sessionName,
          "freeze-queue-immediate",
          s"textLen=${msg.text.length}"
        )
        // 2026-09-27 裁定（ORCH1-R8）：入队改指 TurnBoundary.enqueueImmediateInput
        // （追加语义 :+ 逐字不变）。
        val queued = state.copy(execution = TurnBoundary.enqueueImmediateInput(state.execution, msg))
        IO.pure(
          frozen(agentDef, resources, depth, parentRef, queued, replyTo, resumeAt, reason, retryCount, escalation)
        )

      case AgentCommand.MailQueued(item, _) =>
        // 镜像 processing：计数 +1，实际内容在磁盘（MailQueueStore）。
        // 2026-09-27 裁定（ORCH2-P1）：计数腿与 AgentProcessing 逐字同形 ⇒ 改指
        // BehaviorCommon.mailQueuedCountUp，log 与回本态构造留案体。
        logAgentEvent(
          agentDef,
          depth,
          state.sessionId,
          state.sessionName,
          "mail-queued",
          s"from=${item.from} pendingCount=${state.execution.pendingMailQueueCount + 1}"
        )
        IO.pure(
          frozen(
            agentDef,
            resources,
            depth,
            parentRef,
            BehaviorCommon.mailQueuedCountUp(state),
            replyTo,
            resumeAt,
            reason,
            retryCount,
            escalation
          )
        )

      // 2026-09-13（permshield S1）：`SetSafetyMode` 已退役（见 idle 分支注释），
      // frozen 下由下方 catch-all 兜底（留在 frozen，状态不变）。

      case AgentCommand.UpdateContextWindow(window) =>
        // 轻量存储：恢复后下一次 dispatch 的 autoCompact 自会按新窗口评估溢出。
        updateContextWindowStay(state, window)(s =>
          IO.pure(frozen(agentDef, resources, depth, parentRef, s, replyTo, resumeAt, reason, retryCount, escalation))
        )

      case AgentCommand.UpdateGitBranch(branch) =>
        updateGitBranchStay(state, branch)(s =>
          IO.pure(frozen(agentDef, resources, depth, parentRef, s, replyTo, resumeAt, reason, retryCount, escalation))
        )

      // ctxthresh 批：frozen 态同 UpdateContextWindow 的轻量存储语义——解冻后
      // 下一次 dispatch 自会按新的生效门限评估溢出。
      case AgentCommand.SetCompactThresholdRatio(ratio) =>
        setCompactThresholdRatioStay(state, ratio)(s =>
          IO.pure(frozen(agentDef, resources, depth, parentRef, s, replyTo, resumeAt, reason, retryCount, escalation))
        )

      case n: AgentCommand.BackgroundTaskNotification =>
        // 转成 ExternalEvent 走上面的排队分支（同 processing 的处理方式）。
        forwardBackgroundTaskNotification(n)(
          IO.pure(
            frozen(agentDef, resources, depth, parentRef, state, replyTo, resumeAt, reason, retryCount, escalation)
          )
        )

      case AgentCommand.SetPermissionDeferred(deferred) =>
        // 镜像 processing：持有 deferred，防子 agent 权限应答悬空。
        setPermissionDeferredStay(state, deferred)(s =>
          IO.pure(frozen(agentDef, resources, depth, parentRef, s, replyTo, resumeAt, reason, retryCount, escalation))
        )

      case AgentCommand.SessionStarted(address, agentName, taskDescription) =>
        val session = AgentSessionInfo(address, agentName, taskDescription, "running")
        agentSessionsStay(state, state.agentSessions :+ session)(s =>
          IO.pure(frozen(agentDef, resources, depth, parentRef, s, replyTo, resumeAt, reason, retryCount, escalation))
        )

      case AgentCommand.SessionUpdate(address, status) =>
        val updated = state.agentSessions.map(s => if s.address == address then s.copy(status = status) else s)
        agentSessionsStay(state, updated)(s =>
          IO.pure(frozen(agentDef, resources, depth, parentRef, s, replyTo, resumeAt, reason, retryCount, escalation))
        )

      case AgentCommand.SessionClosed(address) =>
        val updated = state.agentSessions.filterNot(_.address == address)
        agentSessionsStay(state, updated)(s =>
          IO.pure(frozen(agentDef, resources, depth, parentRef, s, replyTo, resumeAt, reason, retryCount, escalation))
        )

      case AgentCommand.CompactionComplete(result) =>
        // stale 压缩结果：frozen 不可能由压缩轮直接进入（压缩 dispatch 在
        // super 内部静态解析，进入 frozen 前已完成）。镜像 idle 的兜底——完成
        // deferred 防等待方悬空，结果本身丢弃。
        // 2026-09-27 裁定（ORCH2-P1）：stale 兜底公共段与 AgentIdle 逐字同形 ⇒
        // 改指 BehaviorCommon.staleCompactionDiscard；deferred 结算文案字面量原样
        // 留案体作实参，本态 Some 腿清 pendingCompaction（与 idle 之设计差）以
        // clearPendingCompaction=true 显式参数化，回本态构造以 stay 续参注入。
        BehaviorCommon.staleCompactionDiscard(
          agentDef,
          depth,
          state,
          result,
          "Compaction result arrived while agent was frozen",
          clearPendingCompaction = true
        )(s =>
          IO.pure(frozen(agentDef, resources, depth, parentRef, s, replyTo, resumeAt, reason, retryCount, escalation))
        )

      case AgentCommand.ClearReadTracker =>
        clearReadTrackerStay(state)(
          IO.pure(
            frozen(agentDef, resources, depth, parentRef, state, replyTo, resumeAt, reason, retryCount, escalation)
          )
        )

      // F（2026-09-25 命令消重）：与 processing 态逐字同形,收敛至
      // AgentActor.resetSessionHandler。
      case AgentCommand.ResetSession =>
        resetSessionHandler(agentDef, resources, depth, parentRef, state)

      case _ =>
        // stale LlmComplete/LlmFailed/ToolsComplete/TriggerCompaction 等 → 丢弃
        // 留在 frozen（这些消息属于被 gate 拦下的 turn，没有 fiber 在等待它们）。
        IO.pure(frozen(agentDef, resources, depth, parentRef, state, replyTo, resumeAt, reason, retryCount, escalation))

  end frozen

end AgentFrozen
