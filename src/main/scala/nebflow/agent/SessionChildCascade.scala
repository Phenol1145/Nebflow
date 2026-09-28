package nebflow.agent

import cats.effect.IO
import cats.syntax.all.*
import nebflow.actor.*
import nebflow.shared.NebflowLogger

/**
 * V1 (2026-09-03, 结果投递链丢失向量修复): session-delete cascade for
 * Delegate/SubTask/Ephemeral child actors.
 *
 * 丢失形态：deleteSession 只停 team 成员（stopTeamSessionActors）+ root agent
 * （removeRootAgent）——Delegate/Ephemeral 子代理继续在飞；子完成后
 * BackoffSupervisor/adapter `parentRef ! ExternalEvent`，而父 actor 循环已退出
 * （ActorSystem doc: "messages sent to it vanish silently"）→ 结果静默蒸发。
 *
 * 取舍（停掉 vs 让其完成落盘）：选「停掉」。父会话删除 = 用户显式放弃该子树
 * 全部产出；(a) 让子跑完其结果也无处投递（父队列文件随会话删除，无人再读），
 * 只会白烧 LLM token；(b) 停止语义清晰无竞态窗口——子代理 Stop 后不再产生
 * 完成事件，BackoffSupervisor 的 Cancelled 兜底亦无从触发死信路径。任务记录
 * 经 cancelRunningForParent 同步终态化（task store 真实性 + V2 启动扫描不会
 * 对已删父会话误报）。
 *
 * 抽为独立对象（原为 WebSocketRoutes 私有方法）：deleteSession/batchDelete
 * 两条 WS 路径与单测共用同一实现。
 */
object SessionChildCascade:
  private val logger = NebflowLogger.forName("nebflow.agent.session-cascade")

  // 2026-09-28 裁定（ORCH4-R1；口径 = `ORCH4-P1` T7 三面 + `ORCH4-P2` 逐点等价，落点 = agent/LifecycleEnds.scala）：本面与
  // SubAgentStartupRecovery / BackoffSupervisor 的终态序列经逐行 + 词法归一两步
  // 判别，**无逐字形同业务子序列**（台账见 LifecycleEnds 对象 doc），故本面
  // **零改指、零重排、动作顺序逐点保持**。本面两处特有点按裁定 ② 留守本站：
  //   ① 终结判定 = `parentSessionId == sessionId && kind ∈ {Delegate,SubTask,
  //      Ephemeral}`（:64-65（HEAD :46-47）；全仓唯一谓词，与 SR 的 `status=="running"`
  //      不同源。⚠ 行号口径（2026-09-28 审计 Finding-1 随迁 re-pin）：本注在对象顶部
  //      插入 18 行 ⇒ 本文件内所有目标行现行坐标 = HEAD + 18；`grep 'parentSessionId =='`
  //      须按**剥注释**口径判（`codeOnly` 先例），否则会命中本注 :34 自身）；
  //   ② `supervisorRef match` 两岔——有 supervisor 走「投递 Cancelled 给监督者」
  //      （现行 :73（HEAD :55，+18）；再汇入 BackoffSupervisor:163-181 的终态序列），
  //      无 supervisor 走本站
  //      降级腿（顺序 = child Stop（现行 :75（HEAD :57））→ 注册表移除（现行 :79（HEAD :61）），**与 BS 终态腿的
  //      注册表移除 → child Stop 相反顺序**，且本站 Stop 带 handleErrorWith、BS 不带 ⇒
  //      裁定 ① 判「不满足逐字同形」，禁统一）。
  // 并置原注（上方对象 doc 的 V1 取舍论证、下方 def doc 的「停止走 doCancel 同款正路」
  // 与 `sup ! Cancelled` 行的「绝不能再包一层 IO(...)」提示）逐字保留，未改一字。

  /**
   * Stop every Delegate/SubTask/Ephemeral child of `sessionId` (registry
   * lookup by parentSessionId), unregister it, and terminalize its in-flight
   * task records. Returns the stopped child session ids.
   *
   * 停止走 AgentControl doCancel 同款正路：有 supervisorRef 的（Delegate/
   * SubTask = BackoffSupervisor）→ `sup ! Cancelled`——supervisor 统一结算
   * （父 ExternalEvent cancelled 通知 → barrier 正确释放、taskStore 终态化、
   * registry 移除、child Stop、自停）；直接 Stop 子 actor 会绕过 supervisor
   * 被当成 crash 触发重启，绝不走这条。无 supervisor 的（Ephemeral 等）→
   * doCancel 同款降级：直接 Stop + registry 移除。cancelRunningForParent
   * 兜底终态化任务记录（supervisor 路已终态化的记录此处天然为空集）。
   */
  def stopChildDelegateActors(resources: SharedResources, sessionId: String): IO[List[String]] =
    resources.agentRegistry.get.flatMap { registry =>
      val children = registry.values.filter { rec =>
        rec.parentSessionId == sessionId &&
        (rec.kind == AgentKind.Delegate || rec.kind == AgentKind.SubTask || rec.kind == AgentKind.Ephemeral)
      }.toList
      children.traverse { rec =>
        logger.info(s"deleteSession: cascade-cancelling child ${rec.kind} actor ${rec.sessionId}") *>
          (rec.supervisorRef match
            case Some(sup) =>
              // 注意：`ref ! msg` 本身返回 IO[Unit]（offer 的描述）——直接使用，
              // 绝不能再包一层 IO(...)（那会得到 IO[IO[Unit]]，内层 offer 永不执行）。
              (sup ! AgentEvent.Cancelled(rec.sessionId, s"parent session $sessionId deleted"))
            case None =>
              (rec.ref ! AgentCommand.Stop(s"parent session $sessionId deleted"))
                .handleErrorWith(e =>
                  logger.warn(s"deleteSession: child stop offer failed for ${rec.sessionId}: ${e.getMessage}")
                ) *>
                resources.agentRegistry.update(_ - rec.sessionId)
          ).as(rec.sessionId)
      } <* resources.subAgentTaskStore
        .cancelRunningForParent(sessionId, s"parent session $sessionId deleted (cascade stop)")
        .flatMap {
          case Nil => IO.unit
          case ids =>
            logger.info(s"deleteSession: cascade-cancelled in-flight task(s) of $sessionId: ${ids.mkString(", ")}")
        }
    }
end SessionChildCascade
