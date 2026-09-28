package nebflow.ir

import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Json, JsonObject}

import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}

/** 计划里一个已判定节点（审计与审批共用）。 */
final case class PlannedNode(
  node: String,
  target: NodeTarget,
  decision: Decision,
  cmd: Option[CommandDef],
  argsDigest: String
)

/**
 * 已校验、已判定的计划（§8.4 步 1–3 的产物）。IR 树本身不复制：执行侧按**节点路径**
 * （§3.3）回查本对象里的元数据，树形与语义因此只有一份。
 */
final case class Plan(
  ir: Ir,
  nodes: Vector[PlannedNode],
  planDigest: String,
  capsDigest: String
):
  private lazy val byNode = nodes.map(n => n.node -> n).toMap

  def node(path: String): Option[PlannedNode] = byNode.get(path)
  def cmd(path: String): Option[CommandDef] = byNode.get(path).flatMap(_.cmd)
  def target(path: String): Option[NodeTarget] = byNode.get(path).map(_.target)

  /** 首个 `Deny`（[I7]：含 `Deny` ⇒ 零执行）。 */
  def denied: Option[PlannedNode] = nodes.find(_.decision.isInstanceOf[Decision.Deny])

  def firstAsk: Option[PlannedNode] = nodes.find(_.decision.isInstanceOf[Decision.Ask])

  def allAllow: Boolean = nodes.forall(_.decision == Decision.Allow)
end Plan

/**
 * 计划校验与判定（标准 §3.3、§5.3、§7.4、§8.4 步 1–3）。
 *
 * 顺序是**规定**的：lowering → 校验 → **整计划逐节点 `decide`** → 有 `Deny` 即停判并
 * 整计划拒绝 → 有 `Ask` 则先看凭据（[[Router]] 的 5a/5b）→ 全 `Allow` 才执行。
 * 「先全判后执行」（[I7]）正是 [D8] 说的一次交互换掉一整类一致性难题的落点。
 *
 * 全流程只在一个地方碰 IO：**路径的 realpath 解析**（[P8]：判定必须在执行前完成，
 * 且判据必须是真实落点 —— 见 [[VfsPath.canonResolved]]）。除此之外全是纯函数，
 * 判定因此可在审批前离线复算（§8.5 的 `planDigest`/`capsDigest` 意义所在）。
 */
object Planner:

  def plan(
    ir: Ir,
    ctx: CallCtx,
    registry: CommandRegistry,
    policy: PolicyEngine,
    limits: IrLimits,
    hmacKey: Array[Byte]
  ): IO[Either[IrError, Plan]] =
    // 纯段（Either 短路）：[L3] → 树形校验 → 逐节点校验 → 流类型匹配
    val prepared: Either[IrError, Vector[PlannedNode]] =
      for
        _ <- llmPlanCheck(ir, ctx)
        _ <- Ir.validate(ir, limits)
        nodes <- collectNodes(ir, ctx, registry, policy, hmacKey)
        _ <- checkStreams(ir, nodes)
      yield nodes

    prepared match
      case Left(err) => IO.pure(Left(err))
      case Right(nodes) =>
        // 唯一的 IO 段：路径 realpath（越根 ⇒ 计划非法 exit 2）。必须在**判定与摘要之前** ——
        // 否则根内一个符号链接就能把 cap 判定与 capsDigest 一起绕开（见 [[VfsPath.canonResolved]]）
        resolvePaths(nodes, ctx.root).map { resolved =>
          for
            rs <- resolved
            judged <- decideAll(rs, ctx, policy)
          yield assemble(ir, judged)
        }

  end plan

  /**
   * [L3]/[D22]（V2 红线 2）：模型输出**永不可**直接派发 IR —— 模型只有 tool_call 一条路
   *  （一次 `Call`），其权限模型与计划路径不同；计划级审批只对 human(P1)/script(P2) 开放。
   */
  private def llmPlanCheck(ir: Ir, ctx: CallCtx): Either[IrError, Unit] =
    if ctx.ingress == Ingress.Llm && !ir.isInstanceOf[Ir.Call] then
      Left(
        IrError
          .invalidArgs("the llm ingress may submit a single Call only ([L3]: the model must not generate plans)")
          .withDetail("reason", "llm_plan_forbidden".asJson)
      )
    else Right(())

  /** 摘帽：planDigest 覆盖 `{ir, plan}`（[D25]），capsDigest 覆盖描述符快照（防 TOCTOU）。 */
  private def assemble(ir: Ir, judged: Vector[PlannedNode]): Plan =
    val planJson = Json.obj("ir" -> Ir.Version.asJson, "plan" -> Ir.encode(ir))
    val capsJson = Json.fromValues(judged.map { n =>
      Json.obj("node" -> n.node.asJson, "caps" -> n.target.capNames.asJson)
    })
    Plan(
      ir = ir,
      nodes = judged,
      planDigest = Canonical.digest(planJson),
      capsDigest = Canonical.digest(capsJson)
    )

  // ── 步 2：逐节点校验（命令存在 / 可见 / args / 路径参数） ────

  private def collectNodes(
    ir: Ir,
    ctx: CallCtx,
    registry: CommandRegistry,
    policy: PolicyEngine,
    hmacKey: Array[Byte]
  ): Either[IrError, Vector[PlannedNode]] =
    Ir.walk(ir)
      .traverse { case (node, sub) =>
        sub match
          case Ir.Call(name, args, literal) =>
            for
              cmd <- registry.get(name).toRight(IrError.unknownCommand(name))
              // §7.4 检查点①：lowering 时（尽早报错）；②在派发时，③在模型工具表之前
              _ <- policy.checkAudience(ctx.ingress, cmd)
              _ <- ArgsSchema.validate(cmd.argsSchema, args)
              pathArgs <- resolvePathArgs(cmd, args)
              _ <- checkLiteral(node, literal, cmd)
            yield Some(
              PlannedNode(
                node,
                NodeTarget(node, name, cmd.caps, args, pathArgs, Some(cmd.binding)),
                Decision.Allow, // 占位：由 decideAll 填真值
                Some(cmd),
                digestFor(cmd, args, hmacKey)
              )
            )
          case Ir.Redirect(op, rawPath, _) =>
            VfsPath.canon(rawPath).map { canon =>
              val caps = op match
                case RedirectOp.In => Set[Cap](Cap.FsRead(canon.render))
                case RedirectOp.Out | RedirectOp.Append => Set[Cap](Cap.FsWrite(canon.render))
              val args = JsonObject("path" -> rawPath.asJson)
              Some(
                PlannedNode(
                  node,
                  NodeTarget(node, NodeTarget.redirectName(op), caps, args, Map("path" -> canon)),
                  Decision.Allow,
                  None,
                  Canonical.digest(Json.fromJsonObject(args))
                )
              )
            }
          case _ => Right(None) // Pipe / Sequence：本身不判定（子节点各自判定）
      }
      .map(_.flatten.toVector)

  /**
   * 路径参数的 **realpath** 解析（[P8]/[P2]/[P7]）：把每个带路径参数的节点（真命令与
   * `Redirect`）的 `pathArgs` 换成真实落点的 canonical 形式，`Redirect` 的 cap 随落点重算。
   * 越出 VFS 根 ⇒ **计划非法**（exit 2 + `router.schema.bad_value`，[P7] 第一级）。
   *
   * 这一步在**判定与摘要之前**：否则根内的一个符号链接就能同时绕开 cap 判定与 capsDigest。
   */
  private def resolvePaths(nodes: Vector[PlannedNode], root: VfsRoot): IO[Either[IrError, Vector[PlannedNode]]] =
    nodes.traverse(n => resolveNode(n, root)).map(_.sequence.map(_.toVector))

  private def resolveNode(n: PlannedNode, root: VfsRoot): IO[Either[IrError, PlannedNode]] =
    if n.target.pathArgs.isEmpty then IO.pure(Right(n))
    else
      n.target.pathArgs.toList
        .traverse { case (key, lexical) => resolveOne(root, key, lexical) }
        .map(_.sequence)
        .map(_.map { pairs =>
          val resolved = pairs.toMap
          // Redirect 的 cap 由 target 计算（§8.1/§5.5 S4）⇒ 落点变了 cap 跟着变
          val caps = if n.cmd.isEmpty then n.target.caps.map(rebaseCap(_, resolved)) else n.target.caps
          n.copy(target = n.target.copy(caps = caps, pathArgs = resolved))
        })

  private def resolveOne(
    root: VfsRoot,
    key: String,
    lexical: VfsPath.Canon
  ): IO[Either[IrError, (String, VfsPath.Canon)]] =
    VfsPath.canonResolved(root, lexical.render).map {
      case Left(err) => Left(err)
      case Right(canon) => Right((key, canon))
    }

  private def rebaseCap(c: Cap, resolved: Map[String, VfsPath.Canon]): Cap =
    val rendered = resolved.getOrElse("path", VfsPath.Canon(Nil)).render
    c match
      case _: Cap.FsRead => Cap.FsRead(rendered)
      case _: Cap.FsWrite => Cap.FsWrite(rendered)
      case other => other

  /** §8.2：含 `Secret` cap 的命令用 HMAC 而非明文 sha256（低熵凭证的明文摘要可离线字典破解）。 */
  private def digestFor(cmd: CommandDef, args: JsonObject, hmacKey: Array[Byte]): String =
    val json = Json.fromJsonObject(args)
    if cmd.hasSecretCap then Canonical.hmacDigest(json, hmacKey) else Canonical.digest(json)

  /**
   * 路径参数 canonical 化（[P8]：判定必须在执行前完成 ⇒ 路由层需要知道哪些 args 是路径）。
   * 越出 VFS 根 ⇒ **计划非法**（exit 2 + `router.schema.bad_value`，[P7] 第一级）。
   */
  private def resolvePathArgs(cmd: CommandDef, args: JsonObject): Either[IrError, Map[String, VfsPath.Canon]] =
    cmd.pathArgs.toList.sorted
      .flatMap(key => args(key).map(v => (key, v)))
      .traverse { case (key, v) =>
        v.asString match
          case None => Left(IrError.invalidArgs(s"path arg '$key' must be a string", List(s".$key")))
          case Some(raw) => VfsPath.canon(raw).map(c => key -> c)
      }
      .map(_.toMap)

  /** 字面量 stdin 的类型必须与命令声明的 `io.stdin` 相容（§5.3：声明只用于管道类型匹配）。 */
  private def checkLiteral(node: String, literal: StdinSource, cmd: CommandDef): Either[IrError, Unit] =
    literal match
      case StdinSource.None => Right(())
      case StdinSource.LiteralText(_) =>
        if cmd.io.stdin.contains(StreamKind.Text) then Right(())
        else Left(mismatch(node, StreamKind.Text, cmd.io.stdin))
      case StdinSource.LiteralJsonl(_) =>
        if cmd.io.stdin.contains(StreamKind.Jsonl) then Right(())
        else Left(mismatch(node, StreamKind.Jsonl, cmd.io.stdin))

  // ── 步 3：管道流类型匹配（§5.3，执行前即可判定 —— 类型是声明出来的，无需运行） ──

  private def checkStreams(ir: Ir, nodes: Vector[PlannedNode]): Either[IrError, Unit] =
    val defs = nodes.flatMap(n => n.cmd.map(c => n.node -> c)).toMap
    ir match
      case Ir.Pipe(stages) =>
        stages
          .zip(stages.drop(1))
          .zipWithIndex
          .traverse { case ((up, down), i) =>
            for
              upKind <- stdoutKind(up, defs, Ir.childPath("0", i))
              downKind <- stdinKind(down, defs, Ir.childPath("0", i + 1))
              _ <-
                if upKind == downKind || (upKind == StreamKind.Jsonl && downKind == StreamKind.Text) then Right(())
                else Left(mismatch(Ir.childPath("0", i + 1), upKind, Some(downKind)))
            yield ()
          }
          .flatMap(_ => stages.zipWithIndex.traverse { case (s, i) => checkStreams(s, nodes) }.void)
      case Ir.Sequence(items) => items.traverse(s => checkStreams(s, nodes)).void
      case Ir.Redirect(op, _, inner) =>
        checkStreams(inner, nodes).flatMap { _ =>
          op match
            case RedirectOp.In =>
              // S3：文件内容必须满足 inner 声明的 io.stdin（None ⇒ 该命令不接受 stdin）
              stdinKind(inner, defs, Ir.childPath("0", 0)) match
                case Right(_) => Right(())
                case Left(_) => Left(mismatch("0.0", StreamKind.Text, None))
            case _ => Right(())
        }
      case _: Ir.Call => Right(())

    end match

  end checkStreams

  /** 节点 stdout 类型：`Redirect` 自身恒 `text`（S7）；`Sequence` 取末项；`Pipe` 取末级。 */
  private def stdoutKind(ir: Ir, defs: Map[String, CommandDef], path: String): Either[IrError, StreamKind] =
    ir match
      case Ir.Call(name, _, _) => defs.get(path).map(_.io.stdout).toRight(IrError.unknownCommand(name))
      case _: Ir.Redirect => Right(StreamKind.Text)
      case Ir.Pipe(stages) => stdoutKind(stages.last, defs, Ir.childPath(path, stages.length - 1))
      case Ir.Sequence(items) => stdoutKind(items.last, defs, Ir.childPath(path, items.length - 1))

  /**
   * 节点 stdin 需求：`Call` 取声明 `io.stdin`；`Redirect(out/append)` 把需求**透传** inner；
   * `Redirect(in)` 的 stdin 由文件顶替 ⇒ **不接受**上游数据；`Sequence` **无单一 stdin**
   * （§3.1「节点间无流连接」）⇒ 上游有产出即 `mismatch`。
   */
  private def stdinKind(ir: Ir, defs: Map[String, CommandDef], path: String): Either[IrError, StreamKind] =
    ir match
      case Ir.Call(name, _, _) =>
        defs.get(path).flatMap(_.io.stdin).toRight(mismatch(path, StreamKind.Jsonl, None))
      case Ir.Redirect(RedirectOp.In, _, _) => Left(mismatch(path, StreamKind.Jsonl, None))
      case Ir.Redirect(_, _, inner) => stdinKind(inner, defs, Ir.childPath(path, 0))
      case Ir.Pipe(stages) => stdinKind(stages.head, defs, Ir.childPath(path, 0))
      case _: Ir.Sequence => Left(mismatch(path, StreamKind.Jsonl, None))

  private def mismatch(node: String, produced: StreamKind, consumed: Option[StreamKind]): IrError =
    IrError(
      Codes.StreamMismatch,
      s"stream type mismatch at $node: producer emits '${StreamKind.wire(produced)}', consumer accepts " +
        consumed.map(k => s"'${StreamKind.wire(k)}'").getOrElse("no stdin")
    ).withDetail("node", node.asJson).withDetail("produced", StreamKind.wire(produced).asJson)

  // ── 步 4：整计划逐节点判定 ──────────────────────────────────

  private def decideAll(
    nodes: Vector[PlannedNode],
    ctx: CallCtx,
    policy: PolicyEngine
  ): Either[IrError, Vector[PlannedNode]] =
    val judged = Vector.newBuilder[PlannedNode]
    var stopped = false
    nodes.foreach { n =>
      if !stopped then
        val d = policy.decide(ctx.tenant, ctx.ingress, n.target, ctx.safety)
        judged += n.copy(decision = d)
        // v1 取值域最小：遇首个 Deny 即停判并整计划拒绝（§8.4）。审批展示的是「已被判定的
        // 节点」而非全计划——该取舍必须在展示中如实呈现（不得暗示"整计划都判过了"）。
        if d.isInstanceOf[Decision.Deny] then stopped = true
    }
    Right(judged.result())
  end decideAll

end Planner

/** 执行步累积（物化管道：每级 stdout 完整生成后才启动下一级，§5.2）。 */
private final case class Step(results: Vector[NodeResult], stdout: Option[StreamValue], failed: Boolean)

/**
 * 物化执行器（标准 §5、§6）。
 *
 * 四条语义要点：
 *  - **物化管道**（[D4]）：v1 命令是进程内函数，天然返回值语义；真流式需背压协议，
 *    成本远超收益。`yes | head` 这类无限生产端**不存在**（生产者必须有界）。
 *  - **fail-fast**（[D5]）：任一级非零退出 ⇒ 立即停止后续级（**偏离 bash** 的取末条语义）。
 *    短路时已完成节点结果全部保留，**禁止**回滚副作用（IR 无事务语义，[D6]）。
 *  - **限额失败不截断**（§5.2）：截断会让下游基于残缺数据做出错误结论。
 *  - **非零退出是值**（[I6]）：只有路由层自身故障才不是节点结果。
 */
object Executor:

  final case class ExecResult(
    results: Vector[NodeResult],
    finalStdout: Option[StreamValue],
    interruptedAt: Option[String]
  )

  def run(plan: Plan, ctx: CallCtx, limits: IrLimits, policy: PolicyEngine): IO[ExecResult] =
    eval(plan.ir, Ir.RootPath, None, plan, ctx, limits, policy).map { step =>
      ExecResult(step.results, step.stdout, if step.failed then step.results.lastOption.map(_.node) else None)
    }

  private def eval(
    ir: Ir,
    path: String,
    stdin: Option[StreamValue],
    plan: Plan,
    ctx: CallCtx,
    limits: IrLimits,
    policy: PolicyEngine
  ): IO[Step] =
    ir match
      case Ir.Call(_, _, literal) => evalCall(path, literal, stdin, plan, ctx, limits, policy)
      case Ir.Pipe(stages) => evalSteps(stages, path, feed = true, stdin, plan, ctx, limits, policy)
      case Ir.Sequence(items) => evalSteps(items, path, feed = false, None, plan, ctx, limits, policy)
      case Ir.Redirect(op, rawPath, inner) =>
        val canon = plan.target(path).flatMap(_.pathArgs.get("path")).getOrElse(VfsPath.Canon(Nil))
        val target = VfsPath.resolve(ctx.root, canon)
        val started = System.nanoTime()
        op match
          case RedirectOp.In =>
            readFile(target).flatMap {
              case Left(err) => IO.pure(shortCircuit(path, NodeTarget.redirectName(op), ExitCode.Failure, err))
              case Right(value) =>
                parseForStdin(value, plan, Ir.childPath(path, 0), rawPath).flatMap {
                  case Left(err) => IO.pure(shortCircuit(path, NodeTarget.redirectName(op), ExitCode.Failure, err))
                  case Right(fed) =>
                    eval(inner, Ir.childPath(path, 0), Some(fed), plan, ctx, limits, policy).map { step =>
                      val own = ownResult(path, op, step, started, None)
                      Step(step.results :+ own, Some(StreamValue.Text("")), step.failed)
                    }
                }
            }
          case RedirectOp.Out | RedirectOp.Append =>
            // 上游（管道）给 Redirect 的数据转交 inner；S2：inner 成功才创建/截断目标文件
            eval(inner, Ir.childPath(path, 0), stdin, plan, ctx, limits, policy).flatMap { step =>
              val innerExit = step.results.lastOption.map(_.exit).getOrElse(ExitCode.Ok)
              val payload = step.stdout.getOrElse(StreamValue.Text(""))
              val write =
                if innerExit != ExitCode.Ok then IO.pure(Right(()))
                else writeFile(target, payload, append = op == RedirectOp.Append)
              write.map { wr =>
                val (exit, err) = wr match
                  case Right(_) => (innerExit, step.results.lastOption.flatMap(_.error))
                  case Left(e) => (ExitCode.Failure, Some(e))
                val own = ownResult(path, op, step, started, Some((exit, err)))
                Step(step.results :+ own, Some(StreamValue.Text("")), exit != ExitCode.Ok)
              }
            }

        end match

  private def evalCall(
    path: String,
    literal: StdinSource,
    stdin: Option[StreamValue],
    plan: Plan,
    ctx: CallCtx,
    limits: IrLimits,
    policy: PolicyEngine
  ): IO[Step] =
    val target = plan.target(path)
    val nodeCtx = ctx
      .withNode(path)
      .copy(
        pathArgs = target.map(_.pathArgs).getOrElse(Map.empty),
        stdin = literal match
          case StdinSource.None => stdin
          case StdinSource.LiteralText(t) => Some(StreamValue.Text(t))
          case StdinSource.LiteralJsonl(items) => Some(StreamValue.Jsonl(items))
      )
    plan.cmd(path) match
      case None => IO.pure(shortCircuit(path, path, ExitCode.UnknownCommand, IrError.unknownCommand(path)))
      case Some(cmd) =>
        // §7.4 检查点②：派发时（纵深防御）。lowering 已拦一次，此处再拦——同一命令
        // 分别从两处尝试都必须被拦（C25）。
        policy.checkAudience(nodeCtx.ingress, cmd) match
          case Left(err) => IO.pure(shortCircuit(path, cmd.name, ExitCode.Usage, err))
          case Right(_) =>
            val started = System.nanoTime()
            invoke(cmd, target.map(_.args).getOrElse(JsonObject.empty), nodeCtx, limits).map { raw =>
              // J6：生产端**必须**只输出声明过的流类型；声明与实况不符 ⇒ `router.stream.violation`
              // （dev: 绑定由 `StreamValue` 承载类型，此处是对「实现与描述符漂移」的兜底闸）。
              val outcome = raw.stdout match
                case Some(v) if raw.exit == ExitCode.Ok && kindOf(v) != cmd.io.stdout =>
                  Outcome(
                    ExitCode.Failure,
                    None,
                    Some(
                      IrError(
                        Codes.StreamViolation,
                        s"command declared stdout=${StreamKind.wire(cmd.io.stdout)} but produced ${StreamKind.wire(kindOf(v))}"
                      ).withDetail("command", cmd.name.asJson)
                    )
                  )
                case _ => raw
              val result = NodeResult(
                node = path,
                command = cmd.name,
                exit = outcome.exit,
                stdout = outcome.stdout,
                error = outcome.error,
                durationMs = (System.nanoTime() - started) / 1000000L
              )
              Step(Vector(result), outcome.stdout, outcome.exit != ExitCode.Ok)
            }

    end match

  end evalCall

  private def evalSteps(
    stages: List[Ir],
    parentPath: String,
    feed: Boolean,
    initial: Option[StreamValue],
    plan: Plan,
    ctx: CallCtx,
    limits: IrLimits,
    policy: PolicyEngine
  ): IO[Step] =
    stages.zipWithIndex
      .foldLeft(IO.pure(Step(Vector.empty, initial, false))) { (acc, si) =>
        val (stage, i) = si
        acc.flatMap { prev =>
          if prev.failed then IO.pure(prev)
          else
            val input = if feed then prev.stdout else None
            eval(stage, Ir.childPath(parentPath, i), input, plan, ctx, limits, policy).map { cur =>
              Step(prev.results ++ cur.results, cur.stdout, cur.failed)
            }
        }
      }

  private final case class Outcome(exit: Int, stdout: Option[StreamValue], error: Option[IrError])

  private def invoke(cmd: CommandDef, args: JsonObject, ctx: CallCtx, limits: IrLimits): IO[Outcome] =
    val run: IO[Either[IrError, StreamValue]] = cmd.binding match
      case Binding.Dev(handler) => handler(args, ctx)
      case _ =>
        IO.pure(Left(IrError.bindingUnavailable(cmd.name, "P0 executes dev: bindings only (§8 分阶段)")))
    run
      .timeoutTo(
        limits.nodeTimeout,
        IO.pure(Left(IrError(Codes.Timeout, s"node timed out after ${limits.nodeTimeout.toSeconds}s")))
      )
      .map {
        case Right(value) => limitChecked(value, limits)
        case Left(err) =>
          val exit = err.code match
            case Codes.Timeout => ExitCode.Timeout
            case Codes.Cancelled => ExitCode.Cancelled
            case _ => ExitCode.Failure
          Outcome(exit, None, Some(err))
      }

  end invoke

  private def limitChecked(value: StreamValue, limits: IrLimits): Outcome =
    val (limit, actual) = value match
      case _: StreamValue.Text => (limits.maxTextBytes, value.size)
      case _: StreamValue.Jsonl => (limits.maxJsonlItems.toLong, value.size)
    if actual > limit then
      Outcome(ExitCode.Failure, None, Some(IrError.stdoutLimit(StreamKind.wire(kindOf(value)), actual, limit)))
    else Outcome(ExitCode.Ok, Some(value), None)

  private def kindOf(v: StreamValue): StreamKind = v match
    case _: StreamValue.Text => StreamKind.Text
    case _: StreamValue.Jsonl => StreamKind.Jsonl

  private def shortCircuit(node: String, command: String, exit: Int, err: IrError): Step =
    Step(Vector(NodeResult(node, command, exit, None, "", Some(err), 0L)), None, true)

  /** Redirect 节点自身的结果：stdout 恒空 text（S7）；exit 默认取 inner 的结果。 */
  private def ownResult(
    path: String,
    op: RedirectOp,
    step: Step,
    startedNanos: Long,
    override_ : Option[(Int, Option[IrError])]
  ): NodeResult =
    val last = step.results.lastOption
    val (exit, err) = override_.getOrElse((last.map(_.exit).getOrElse(ExitCode.Ok), last.flatMap(_.error)))
    NodeResult(
      node = path,
      command = NodeTarget.redirectName(op),
      exit = exit,
      stdout = Some(StreamValue.Text("")),
      error = err,
      durationMs = (System.nanoTime() - startedNanos) / 1000000L
    )

  end ownResult

  /** S3：读文件作为 inner 的 stdin。文件不存在 ⇒ exit 1 + `command.failed`（数据问题，不是计划非法）。 */
  private def readFile(target: os.Path): IO[Either[IrError, StreamValue]] =
    IO.blocking {
      if !os.exists(target) || os.isDir(target) then
        Left(
          IrError
            .commandFailed(s"redirect source does not exist: ${target.toString}")
            .withDetail("path", target.toString.asJson)
        )
      else
        decodeUtf8(os.read.bytes(target)) match
          case Left(_) =>
            Left(
              IrError
                .commandFailed(s"file is not valid UTF-8: ${target.toString}")
                .withDetail("path", target.toString.asJson)
                .withDetail("reason", "binary_not_supported".asJson)
            )
          case Right(text) => Right(StreamValue.Text(text))
    }.handleErrorWith(e => IO.pure(Left(IrError.commandFailed(e.getMessage))))

  /**
   * S3 的类型面：`jsonl` 消费端按 §4.3 逐行解析（J2/J5：一行两值、跨行 pretty-print、
   * 非 JSON 行**都**是 `router.stream.violation`，**禁止**静默丢弃）。
   */
  private def parseForStdin(
    value: StreamValue,
    plan: Plan,
    innerPath: String,
    rawPath: String
  ): IO[Either[IrError, StreamValue]] =
    plan.cmd(innerPath) match
      case Some(cmd) if cmd.io.stdin.contains(StreamKind.Jsonl) =>
        IO.pure {
          val lines = value.asText.split("\n", -1).toList.map(_.trim).filter(_.nonEmpty)
          lines
            .traverse(l => io.circe.parser.parse(l).left.map(_ => l))
            .left
            .map { bad =>
              IrError(Codes.StreamViolation, s"redirect source is not JSONL (bad line: ${bad.take(40)})")
                .withDetail("path", rawPath.asJson)
            }
            .map(StreamValue.Jsonl.apply)
        }
      case _ => IO.pure(Right(value))

  /** S2 先判后写 + S5 父目录不存在 ⇒ exit 1 + `command.failed`（**禁止**隐式创建父目录）。 */
  private def writeFile(target: os.Path, value: StreamValue, append: Boolean): IO[Either[IrError, Unit]] =
    IO.blocking {
      val parent = target / os.up
      if !os.exists(parent) then
        Left(
          IrError
            .commandFailed(s"redirect parent directory does not exist: ${parent.toString} (not created implicitly)")
            .withDetail("path", target.toString.asJson)
        )
      else
        os.write.over(target, appendContent(target, value, append), createFolders = true)
        Right(())
    }.handleErrorWith(e => IO.pure(Left(IrError.commandFailed(e.getMessage))))

  private def appendContent(target: os.Path, value: StreamValue, append: Boolean): String =
    val text = value match
      case StreamValue.Text(v) => v
      case j: StreamValue.Jsonl => j.asText
    if append && os.exists(target) && os.size(target) > 0 then "\n" + text else text

  /** 严格 UTF-8（§4.4：**禁止**用替换字符静默降级）。 */
  def decodeUtf8(bytes: Array[Byte]): Either[Throwable, String] =
    try
      val decoder = StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
      Right(decoder.decode(java.nio.ByteBuffer.wrap(bytes)).toString)
    catch case e: CharacterCodingException => Left(e)

end Executor
