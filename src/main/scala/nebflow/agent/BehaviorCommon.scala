/* 编排收敛第二批（ORCH2，预批裁定 ORCH2-P1 执行；RestartAgent 站=P2 流程升级裁定、P1 参数化差异）新建：AgentIdle /
 * AgentProcessing / AgentFrozen 三 behavior 中同一命令逐字同形公共段的共享
 * handler。dispatch 骨架（match 结构与 case 顺序）不动，仅 case 体改指；
 * 各态尾腿/中间腿/恒值实参以参数显式化，逐命令逐状态的副作用序列与分支
 * 目标状态与收敛前逐字等价（behavior 逐字节保持）。
 * 两站措辞不同的带日期裁定注释按作者裁定注释迁移铁律（一字不改、不删、不合并）**逐字并置**于共享体
 * （不合并、不删、各自源上下文保留）。
 */
package nebflow.agent

import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import io.circe.syntax.*
import nebflow.actor.*
import nebflow.shared.*

private[agent] object BehaviorCommon:
  import nebflow.agent.AgentActor.*

  // ============================================================
  // Interrupt（ORCH2-P1）
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH2-P1）：AgentProcessing Interrupt 与 AgentFrozen
   * Interrupt 的公共段逐字同形（cancel→Interrupted 帧→压缩终局帧→deferred
   * 结算→markTeamIdle→registry Idle，尾段 yield 终态逐字同：
   * dropCompactionScratch.resetForInterrupt.withCurrentTurnId(+1)
   * .withPendingCompaction(None) ⇒ 回 idle）⇒ 提取本共享 handler。差异以
   * afterRegistry 实参显式参数化（processing 尾腿 = #250 pending 槽回收；
   * frozen 尾腿 = registry frozenReason 清位），log detail 字面量两站不同 ⇒
   * 留各案体。idle 态 Interrupt = no-op 留本态，真语义差异（ORCH2-P3，
   * AgentIdle 行侧注记，不并入本腿）。
   */
  private[agent] def interruptToIdle(
    agentDef: AgentDef,
    resources: SharedResources,
    depth: Int,
    parentRef: Option[ActorRef[AgentCommand]],
    state: AgentState
  )(afterRegistry: IO[Unit])(using ctx: ActorContext[AgentCommand]): IO[Behavior[AgentCommand]] =
    for
      _ <- ctx.cancelCurrentTurn()

      _ <- emitStream(state.wsSend, AgentStreamEvent.Interrupted, isSubagent = depth > 0, state.sessionId)

      // compactui 批（2026-09-15 事故，作者 16:45–16:48 实证）：压缩作业被中断
      // 必须补发**终局帧**——否则客户端 pill 永挂、落盘孤儿 chat.compacting
      // 每次历史重放复活。详见 emitAbandonedCompaction 文档。
      // compactui 批（2026-09-15 事故）：与 processing 的 Interrupt 同款终局帧
      // （此处原先同样只清 pendingCompaction 不发帧）。
      _ <- AgentCompactionHandlers.emitAbandonedCompaction(state, depth)

      _ <- state.pendingCompaction
        .flatMap(_.replyDeferred)
        .traverse_(d => d.complete(Left("Interrupted by user")).void.handleErrorWith(_ => IO.unit))
      // Back to idle without finishing the turn — clear the busy mark so a
      // interrupted team agent isn't stuck "running" in the Teams panel.
      // Back to idle without resuming — clear the team busy mark so a
      // frozen-then-interrupted team agent isn't stuck "running".
      _ <- AgentFinishTurn.markTeamIdle(agentDef, state.sessionId)
      // R2 closure (wait-timeout-fix): user cancel is the guaranteed exit
      // from WaitingForUser (AskUser/permission parks the turn fiber on a
      // deferred — no finishTurnCont runs). Without this touch the registry
      // would keep the waiting status forever. Idle — NOT Processing — so
      // the watcher sees a consistent idle row (also fixes the pre-existing
      // stale-Processing-after-interrupt gap).
      // frozen 专属：registry 退回 Idle——否则 FreezeScheduler 会持续 ping
      // 一个已回 idle 的 agent（无害但浪费，且面板显示错误）。
      _ <- touchRegistryActivity(resources, state.sessionId, AgentStatus.Idle)
      // 各态尾腿差异（ORCH2-P1 参数化）：processing = #250 pending 槽回收；
      // frozen = updateRegistryFrozenReason(None)——实参构造留守各案体。
      _ <- afterRegistry
    yield
      // Hard-recovery P3: cancelCurrentTurn is now fire-and-forget — the
      // abandoned turn fiber may still complete and send a late
      // LlmComplete/LlmFailed. Bump currentTurnId so the stale-turnId
      // guard discards them (turnId is monotonic; the next dispatch takes
      // +1 from here with no collision — AgentCore.pipeLlmCall 的
      // `currentTurnId + 1` 取号处；2026-09-25 行号引用修正)。
      // Hard-recovery P3: same stale-turnId bump as the processing-state
      // Interrupt — fire-and-forget cancel admits late turn results.
      // compactui 批（2026-09-15 事故 ②）：dropCompactionScratch 必须在
      // withPendingCompaction(None) **之前**应用（判据依赖作业仍在）。
      // compactui 批（2026-09-15 事故 ②）：同 processing 面，摘压缩轮临时输入。
      val interruptedState = AgentCompactionHandlers
        .dropCompactionScratch(state)
        .resetForInterrupt
        .withCurrentTurnId(state.execution.currentTurnId + 1)
        .withPendingCompaction(None)
      AgentIdle.idle(agentDef, resources, depth, parentRef, interruptedState)
    end for
  end interruptToIdle

  // ============================================================
  // Stop（ORCH2-P1）
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH2-P1）：三态 Stop 公共序列（cancel→kill 会话 shell
   * 进程→〔态中腿〕→lifecycle stop hooks→stopped）除 frozen 的两枚 registry
   * 回写外逐字同形 ⇒ 提取本共享序列；中间差异以 betweenKillAndHooks 实参
   * 显式参数化（idle/processing = IO.unit 恒值实参——参数化退化形态、非
   * 语义统一；frozen = registry 退回 Idle + frozenReason 清位）。log detail
   * 字面量各态不同 ⇒ 留各案体（idle 态本无 logAgentEvent ⇒ 其案体不补）。
   */
  private[agent] def stopSequence(
    resources: SharedResources,
    state: AgentState
  )(betweenKillAndHooks: IO[Unit])(using ctx: ActorContext[AgentCommand]): IO[Behavior[AgentCommand]] =
    for
      _ <- ctx.cancelCurrentTurn()

      _ <- nebflow.core.tools.BgTaskRegistry.reclaimSession(state.sessionId, state.wsSend, state.rootSessionId)
      _ <- betweenKillAndHooks
      _ <- fireLifecycleStopHooks(resources, state)
    yield Behaviors.stopped
  end stopSequence

  // ============================================================
  // RestartAgent（ORCH2-P1 · P2 升级裁定）
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH2-P1 · P2 升级裁定）：processing/frozen 两态 RestartAgent 除末端
   * dispatch 状态变换外逐字同形（cancel→压缩轮 deferred 结算→Interrupted
   * 帧→restartStateFor→agentRestarted WS 帧→pipeLlmCall(Gated)）⇒ 提取本
   * 共享 handler，差异集中到调用点显式实参（processing =
   * _.withNextLoopTurn；frozen = identity——冻结时段 supervisor 重启不唤醒、
   * 不刷新 loop 纪元，设计注释留守 AgentFrozen 案体）。idle 态 RestartAgent
   * 少三腿（无 cancel/deferred 结算/Interrupted 帧）⇒ 真语义差异
   * （ORCH2-P3，AgentIdle 行侧注记，不并入本腿）。
   */
  private[agent] def restartAgentCore(
    agentDef: AgentDef,
    resources: SharedResources,
    depth: Int,
    parentRef: Option[ActorRef[AgentCommand]],
    state: AgentState,
    level: RestartLevel
  )(dispatchState: AgentState => AgentState)(using ctx: ActorContext[AgentCommand]): IO[Behavior[AgentCommand]] =
    for
      _ <- ctx.cancelCurrentTurn()
      _ <- state.pendingCompaction
        .flatMap(_.replyDeferred)
        .traverse_(d => d.complete(Left("Restarted by supervisor")).void.handleErrorWith(_ => IO.unit))
      _ <- emitStream(state.wsSend, AgentStreamEvent.Interrupted, isSubagent = depth > 0, state.sessionId)
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
      result <- AgentProcessing.pipeLlmCall(agentDef, resources, depth, parentRef, dispatchState(restartState), None)
    yield result
    end for
  end restartAgentCore

  // ============================================================
  // MailQueued 计数腿（ORCH2-P1）
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH2-P1）：processing/frozen 两态 MailQueued 计数腿
   * 逐字同形（pendingMailQueueCount 累加不钳制——#10 (2026-08-27
   * mail-queue wedge) 语义），log 语句与回本态行为构造留守各案体。
   */
  private[agent] def mailQueuedCountUp(state: AgentState): AgentState =
    state.copy(execution = state.execution.copy(pendingMailQueueCount = state.execution.pendingMailQueueCount + 1))

  // ============================================================
  // CompactionComplete stale 兜底（ORCH2-P1）
  // ============================================================

  /**
   * 2026-09-27 裁定（ORCH2-P1）：idle 与 frozen 两态 CompactionComplete 的
   * stale 兜底公共段逐字同形（"stale-compaction-discarded" 日志 + detail
   * fold、缓存失效、deferred 失败结算、结果丢弃、留在本态）⇒ 提取本共享
   * handler。两态真差异显式参数化：staleReplyMsg = deferred 失败结算文案
   * （两站字面量不同 ⇒ 原样留各案体作实参）；clearPendingCompaction = Some
   * 腿回本态前是否清压缩作业（frozen=true / idle=false——设计差，不统一）；
   * 回本态构造以 stay 续参注入各态「留在本态」。processing 态为真压缩收尾
   * 面 ⇒ 真语义差异（ORCH2-P3，AgentProcessing 行侧注记，不并入本腿）。
   */
  private[agent] def staleCompactionDiscard(
    agentDef: AgentDef,
    depth: Int,
    state: AgentState,
    result: Either[String, List[Message]],
    staleReplyMsg: String,
    clearPendingCompaction: Boolean
  )(stay: AgentState => IO[Behavior[AgentCommand]])(using ctx: ActorContext[AgentCommand]): IO[Behavior[AgentCommand]] =
    logAgentEvent(
      agentDef,
      depth,
      state.sessionId,
      state.sessionName,
      "stale-compaction-discarded",
      result.fold(err => s"err=${err.take(60)}", msgs => s"ok=${msgs.size}msgs")
    )
    // Compaction finished (even if stale): messages shrank, the cached
    // systemStable is rebuilt on the next turn.
    val staleState = state.invalidateSystemStableCache
    state.pendingCompaction.flatMap(_.replyDeferred) match
      case Some(d) =>
        ctx.forkTurn(
          d.complete(Left(staleReplyMsg))
            .void
            .handleErrorWith(_ => IO.unit)
        ) *> stay(
          if clearPendingCompaction then staleState.withPendingCompaction(None) else staleState
        )
      case None => stay(staleState)
    end match
  end staleCompactionDiscard

end BehaviorCommon
