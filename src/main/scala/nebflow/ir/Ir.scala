package nebflow.ir

import io.circe.syntax.*
import io.circe.{Json, JsonObject}

import scala.concurrent.duration.*

/** `Call` 的 stdin 字面量（R5）：单键 `text` 或 `jsonl`。 */
enum StdinSource:
  case None
  case LiteralText(text: String)
  case LiteralJsonl(items: List[Json])

/** `Redirect` 目标模式（R8）。 */
enum RedirectOp:
  case In, Out, Append

object RedirectOp:

  def wire(op: RedirectOp): String = op match
    case In => "in"
    case Out => "out"
    case Append => "append"

  def parse(raw: String): Option[RedirectOp] = raw match
    case "in" => Some(In)
    case "out" => Some(Out)
    case "append" => Some(Append)
    case _ => None

end RedirectOp

/**
 * 能力调用中间表示（标准 §3.1，**四个节点刻意保持最小**）。
 *
 * `Sequence` 而非 `Seq`：避免在 Scala 作用域内遮蔽 `scala.Seq`（[D1]）。
 * v1 无控制流（[I4]）：无变量/条件/循环/函数/命令替换/通配展开。
 */
enum Ir:
  case Call(name: String, args: JsonObject, stdin: StdinSource = StdinSource.None)
  case Pipe(stages: List[Ir])
  case Sequence(items: List[Ir])
  case Redirect(op: RedirectOp, path: String, inner: Ir)

object Ir:

  /** 线格式版本（信封携带；节点内不带 —— 版本是信封属性，§10.1）。权威常量在 [[IrWire]]。 */
  val Version = IrWire.Version

  /** 四个标签键（§3.2 单键标签）。 */
  val TagKeys: Set[String] = Set("call", "pipe", "sequence", "redirect")

  // ── 编码 ──────────────────────────────────────────────────

  def encode(ir: Ir): Json = ir match
    case Ir.Call(name, args, stdin) =>
      val base = Json.obj("name" -> name.asJson, "args" -> Json.fromJsonObject(args))
      val withStdin = stdin match
        case StdinSource.None => base
        case StdinSource.LiteralText(text) => base.deepMerge(Json.obj("stdin" -> Json.obj("text" -> text.asJson)))
        case StdinSource.LiteralJsonl(items) =>
          base.deepMerge(Json.obj("stdin" -> Json.obj("jsonl" -> Json.fromValues(items))))
      Json.obj("call" -> withStdin)
    case Ir.Pipe(stages) => Json.obj("pipe" -> Json.obj("stages" -> Json.fromValues(stages.map(encode))))
    case Ir.Sequence(items) => Json.obj("sequence" -> Json.obj("items" -> Json.fromValues(items.map(encode))))
    case Ir.Redirect(op, path, inner) =>
      Json.obj(
        "redirect" -> Json.obj(
          "op" -> RedirectOp.wire(op).asJson,
          "path" -> path.asJson,
          "inner" -> encode(inner)
        )
      )

  // ── 解码（§3.3 严格解析：未知键/未知枚举值/未知版本一律拒绝，不降级不猜测 [I8]） ──

  def decode(json: Json): Either[IrError, Ir] =
    json.asObject match
      case Some(obj) => decodeNode(obj)
      case None => Left(IrError.nodeTag("an IR node must be a JSON object"))

  def parse(text: String): Either[IrError, Ir] =
    io.circe.parser
      .parse(text)
      .left
      .map(e => IrError.nodeTag(s"not valid JSON: ${e.message}"))
      .flatMap(decode)

  private def decodeNode(obj: JsonObject): Either[IrError, Ir] =
    val unknown = obj.keys.filterNot(TagKeys.contains).toList.sorted
    if unknown.nonEmpty then Left(IrError.unknownKey(unknown.head))
    else
      val present = TagKeys.filter(obj.contains).toList
      if present.length != 1 then
        Left(IrError.nodeTag(s"a node must carry exactly one of call|pipe|sequence|redirect (found ${present.length})"))
      else
        present.head match
          case "call" => decodeCall(obj("call").get)
          case "pipe" => decodePipe(obj("pipe").get)
          case "sequence" => decodeSequence(obj("sequence").get)
          case _ => decodeRedirect(obj("redirect").get)

  private def decodeCall(value: Json): Either[IrError, Ir] =
    decodeRecord(value, Set("name", "args", "stdin"), "call").flatMap { o =>
      o("name").flatMap(_.asString) match
        case None => Left(IrError.invalidArgs("call.name is required and must be a string", List(".name")))
        case Some(name) =>
          for
            _ <- Names.syntax(name)
            args <- o("args") match
              case None => Right(JsonObject.empty)
              case Some(j) =>
                j.asObject.toRight(IrError.invalidArgs("call.args must be a JSON object", List(".args")))
            stdin <- o("stdin") match
              case None => Right(StdinSource.None)
              case Some(j) => decodeStdin(j)
          yield Ir.Call(name, args, stdin)
    }

  /** R5：单键 `text`|`jsonl`；未知内键按 R2 走 unknown_key。 */
  private def decodeStdin(value: Json): Either[IrError, StdinSource] =
    decodeRecord(value, Set("text", "jsonl"), "call.stdin").flatMap { o =>
      if o.size != 1 then Left(IrError.invalidArgs("call.stdin must carry exactly one of text|jsonl", List(".stdin")))
      else
        o("text") match
          case Some(j) =>
            j.asString
              .map(StdinSource.LiteralText.apply)
              .toRight(IrError.invalidArgs("call.stdin.text must be a string", List(".stdin.text")))
          case None =>
            o("jsonl").get.asArray
              .map(_.toList)
              .map(StdinSource.LiteralJsonl.apply)
              .toRight(IrError.invalidArgs("call.stdin.jsonl must be an array", List(".stdin.jsonl")))
    }

  private def decodePipe(value: Json): Either[IrError, Ir] =
    decodeRecord(value, Set("stages"), "pipe").flatMap { o =>
      o("stages") match
        case None => Left(IrError.invalidArgs("pipe.stages is required", List(".stages")))
        case Some(j) =>
          j.asArray match
            case None => Left(IrError.invalidArgs("pipe.stages must be an array", List(".stages")))
            case Some(stages) =>
              if stages.length < 2 then
                Left(IrError.invalidArgs("pipe.stages needs at least 2 stages (R7)", List(".stages")))
              else sequence(stages.toList.map(decode)).map(s => Ir.Pipe(s))
    }

  private def decodeSequence(value: Json): Either[IrError, Ir] =
    decodeRecord(value, Set("items"), "sequence").flatMap { o =>
      o("items") match
        case None => Left(IrError.invalidArgs("sequence.items is required", List(".items")))
        case Some(j) =>
          j.asArray match
            case None => Left(IrError.invalidArgs("sequence.items must be an array", List(".items")))
            case Some(items) =>
              if items.isEmpty then
                Left(IrError.invalidArgs("sequence.items needs at least 1 item (R7)", List(".items")))
              else sequence(items.toList.map(decode)).map(s => Ir.Sequence(s))
    }

  private def decodeRedirect(value: Json): Either[IrError, Ir] =
    decodeRecord(value, Set("op", "path", "inner"), "redirect").flatMap { o =>
      val op = o("op").flatMap(_.asString) match
        case None => Left(IrError.invalidArgs("redirect.op is required", List(".op")))
        case Some(raw) =>
          RedirectOp
            .parse(raw)
            .toRight(IrError.badValue(s"unknown redirect.op '$raw' (v1: in|out|append) — strict rejection [I8]"))
      for
        opv <- op
        path <- o("path").flatMap(_.asString) match
          case None => Left(IrError.invalidArgs("redirect.path is required and must be a string", List(".path")))
          case Some(p) => Right(p)
        inner <- o("inner") match
          case None => Left(IrError.invalidArgs("redirect.inner is required", List(".inner")))
          case Some(j) => decode(j)
      yield Ir.Redirect(opv, path, inner)
    }

  /** 单个 JSON 对象的严格字段集（未知键 → R2；非对象 → invalid_args）。 */
  private def decodeRecord(value: Json, allowed: Set[String], where: String): Either[IrError, JsonObject] =
    value.asObject match
      case None => Left(IrError.invalidArgs(s"$where must be a JSON object", List(s".$where")))
      case Some(o) =>
        val unknown = o.keys.filterNot(allowed.contains).toList.sorted
        if unknown.nonEmpty then Left(IrError.unknownKey(unknown.head)) else Right(o)

  private def sequence[A](xs: List[Either[IrError, A]]): Either[IrError, List[A]] =
    xs.foldRight[Either[IrError, List[A]]](Right(Nil)) { (e, acc) =>
      for
        a <- e
        rest <- acc
      yield a :: rest
    }

  // ── 树形规则（§3.3 R6/R9、§5.5 S6；解码只保证单节点形状） ──

  def validate(ir: Ir, limits: IrLimits): Either[IrError, Unit] =
    for
      _ <- checkLimits(ir, limits)
      _ <- checkTree(ir)
    yield ()

  private def checkLimits(ir: Ir, limits: IrLimits): Either[IrError, Unit] =
    val nodes = nodeCount(ir)
    if nodes > limits.maxNodes then
      Left(
        IrError(Codes.LimitNodes, s"plan has $nodes nodes (limit ${limits.maxNodes})")
          .withDetail("nodes", nodes.asJson)
          .withDetail("limit", limits.maxNodes.asJson)
      )
    else
      val d = depth(ir)
      if d > limits.maxDepth then
        Left(
          IrError(Codes.LimitDepth, s"plan depth $d exceeds ${limits.maxDepth}")
            .withDetail("depth", d.asJson)
            .withDetail("limit", limits.maxDepth.asJson)
        )
      else Right(())

    end if

  end checkLimits

  /** R6：一个 Call 的有效 stdin 来源至多一个（上游管道 / stdin 字面量 / Redirect(in)）。 */
  private def checkTree(ir: Ir): Either[IrError, Unit] =
    ir match
      case _: Ir.Call => Right(())
      case Ir.Pipe(stages) =>
        val conflicts = stages.zipWithIndex.collect {
          case (Ir.Call(_, _, stdin), i) if i > 0 && stdin != StdinSource.None => i
          case (Ir.Redirect(RedirectOp.In, _, _), i) if i > 0 => i
          case (Ir.Redirect(RedirectOp.In, _, Ir.Call(_, _, stdin)), 0) if stdin != StdinSource.None => 0
        }
        if conflicts.nonEmpty then
          Left(
            IrError
              .invalidArgs(
                "stdin conflict: a pipe already provides stdin for non-head stages ([R6])",
                conflicts.map(i => s".stages.$i")
              )
              .withDetail("reason", "stdin_conflict".asJson)
          )
        else
          stages.zipWithIndex.collectFirst {
            case (r: Ir.Redirect, i) if i < stages.length - 1 =>
              IrError
                .badValue("redirect must not appear at a non-final pipe stage ([S6]: its stdout is always empty)")
                .withDetail("stage", i.asJson)
          } match
            case Some(err) => Left(err)
            case None => stages.foldLeft[Either[IrError, Unit]](Right(()))((acc, s) => acc.flatMap(_ => checkTree(s)))
        end if
      case Ir.Sequence(items) =>
        items.foldLeft[Either[IrError, Unit]](Right(()))((acc, s) => acc.flatMap(_ => checkTree(s)))
      case Ir.Redirect(_, _, inner) =>
        inner match
          case _: Ir.Sequence =>
            Left(IrError.badValue("redirect must not wrap sequence ([S6]: no single stdout target)"))
          case _ => checkTree(inner)

  def nodeCount(ir: Ir): Int = ir match
    case _: Ir.Call => 1
    case Ir.Pipe(stages) => 1 + stages.map(nodeCount).sum
    case Ir.Sequence(items) => 1 + items.map(nodeCount).sum
    case Ir.Redirect(_, _, inner) => 1 + nodeCount(inner)

  def depth(ir: Ir): Int = ir match
    case _: Ir.Call => 1
    case Ir.Pipe(stages) => 1 + stages.map(depth).max
    case Ir.Sequence(items) => 1 + items.map(depth).max
    case Ir.Redirect(_, _, inner) => 1 + depth(inner)

  /** 子节点的节点路径增量（§3.3：`Pipe.stages[i]`/`Sequence.items[i]` 同为 `父.i`；`Redirect.inner` 恒 `0.0`）。 */
  def childrenOf(ir: Ir): List[(Int, Ir)] = ir match
    case _: Ir.Call => Nil
    case Ir.Pipe(stages) => stages.zipWithIndex.map { case (s, i) => (i, s) }
    case Ir.Sequence(items) => items.zipWithIndex.map { case (s, i) => (i, s) }
    case Ir.Redirect(_, _, inner) => List((0, inner))

  val RootPath = "0"

  def childPath(parent: String, index: Int): String = s"$parent.$index"

  /** 先序遍历（节点路径 → 节点），审批展示与错误定位共用。 */
  def walk(ir: Ir, path: String = RootPath): List[(String, Ir)] =
    (path, ir) :: childrenOf(ir).flatMap { case (i, c) => walk(c, childPath(path, i)) }

end Ir

/**
 * 计划限额（R9 / §5.2）。默认值即标准「建议」值：节点 64、深度 8、text 1 MiB、
 * jsonl 10⁴ 条；超限**失败**，**禁止**静默截断（§5.2：截断会让下游基于残缺数据
 * 做出错误结论）。
 */
final case class IrLimits(
  maxNodes: Int = 64,
  maxDepth: Int = 8,
  maxTextBytes: Long = 1L << 20,
  maxJsonlItems: Int = 10000,
  nodeTimeout: FiniteDuration = 60.seconds
)

object IrLimits:
  val default: IrLimits = IrLimits()
end IrLimits
