package nebflow.ir

import io.circe.parser
import io.circe.syntax.*
import io.circe.{Json, JsonObject}

/**
 * 人侧 argv 糖（标准 §9 ingress #1，P1）：`/<command> [argv…]` → `Call`。
 *
 * 三件套：[[humanShape]]（该不该 lower 的**纯文本形态判定**，gateway 分流闸用）、
 * [[tokenize]] / [[bind]]（argv 文法）、[[lower]]（形态 → 语法 → 查表 → 绑定的总装）。
 * 纯代码、零 IO、不 import core/gateway/cli（层序 `shared < actor < ir < core`；
 * `cli/CliRouter.parseArgs` 是 `private[cli]` 且入参已是 `List[String]`，只作参照不复用）。
 *
 * 文法（§9 最小口径，P2 冻结项之外刻意最小）：
 *
 * ```
 * argv ::= `--key value` | `--key`（仅 isFlag 参数） | `--json <object>` | positional
 * ```
 *
 *  - **tokenize**：按 ASCII 空白（空格/`\t`/`\n`/`\r`）切分、连续空白折叠；P1 **无引号/
 *    无转义/无元字符**——引号字符是普通 token 字符（§9:549 把引号与转义的精确规则
 *    冻结给 P2 单独成文），裸 `--` 是未知 flag，单 dash token（`-x`）是 positional
 *    （无短 flag）。多行 content 整体当一个命令行：换行后的词成为 argv（多半
 *    `too_many_positionals` 报错，fail-closed）。
 *  - **params 是 argv 唯一权威面**（§7.1）：`--key` 的 key 必须 ∈ `cmd.params`；
 *    `argsSchema.properties` 里有但 params 没有的键不可用 `--key` 设置——**禁止按名字
 *    模糊匹配**。无法唯一绑定 ⇒ exit 2 + `router.invalid_args`，禁止猜测。
 *  - **`--json` 是 [L4] 逃生口**：出现时 argv 必须恰为 `[--json, value]` 两 token，
 *    与任何其它实参并存 ⇒ `invalid_args(reason=json_exclusive)`（C24 糖面镜像）；
 *    value 必须是 JSON object，否则 `invalid_args(reason=json_not_object)`。含空格值/
 *    非字符串类型只能走紧凑 `--json` 或直连 `ir` 帧。
 *  - **值域**：糖绑定的值一律是 JSON 字符串（schema 要 number 者须走 `--json`）；
 *    `default` **不注入**（[D19]：`dev:fs:ls` 不带参 ⇒ args 无 `path` 键 ⇒ 列 VFS 根，
 *    `dev:fs:cat` 不带参 ⇒ 读 stdin——缺省与显式 `.` 必须可区分）。required/类型/
 *    `additionalProperties:false` 校验由 Planner 的 `ArgsSchema.validate` 单点把关，
 *    binder 不重复消费。
 *  - **audience 检查不做**：Planner/Executor 双检查点（§7.4）已在 submit 路径上。
 */
object ArgvSugar:

  /** 首段四值 = `Binding.namespace` 的词表（§7.3 N1；`Descriptors.scala` Binding.namespace）。 */
  val Namespaces: Set[String] = Set("dev", "mcp", "ext", "bash")

  /** ASCII 空白（tokenize/humanShape 同一套；不含 `\f`/`\v`，刻意与 §9 口径一致）。 */
  private def isSpace(c: Char): Boolean = c == ' ' || c == '\t' || c == '\n' || c == '\r'

  // ── 形态判定（分流闸） ─────────────────────────────────────

  /**
   * 「未被任何既有闸消费的 `/…`」的**正向判定**（§9:547）：trimmed 以 `/` 起头 ∧ 不以
   * `//` 起头（`InputMentions` 的 `//` 转义边界）∧ 首 token 去 `/` 后按 `:` 切分 ≥ 2 段
   * ∧ 首段 ∈ [[Namespaces]]（**仅小写精确匹配**，禁大小写折叠，N2）。
   *
   * 这是白名单的补即保留名集合：`/clear`、`/compact`、`/ask`、`/onboarding`、`/skill:*`
   * 天然因「首段 ∉ 四值或单段」被拒——无需另维护保留名清单（与现有 slash 名零交集，
   * 新增保留名无需改本闸）。**不校验完整名语法**：`/dev:FS:ls` 在此为 true、在
   * [[lower]] 里按 R3 报 `name_invalid`（exit 2），`/dev:fs:nope` 报 `unknown_command`
   * （exit 127）——两档与直连 `ir` 腿同码（§6.2/§6.4）。
   */
  def humanShape(text: String): Boolean =
    val trimmed = text.trim
    trimmed.startsWith("/") && !trimmed.startsWith("//") && {
      val name = trimmed.takeWhile(c => !isSpace(c)).drop(1)
      val segments = name.split(":", -1)
      segments.length >= 2 && Namespaces.contains(segments(0))
    }

  // ── tokenizer ─────────────────────────────────────────────

  /** 按 ASCII 空白切分、连续空白折叠；空/全空白 ⇒ `Nil`。 */
  def tokenize(line: String): List[String] =
    line.split("[ \t\n\r]+").toList.filter(_.nonEmpty)

  // ── 绑定器 ────────────────────────────────────────────────

  /**
   * argv → args（§9 最小口径）。规则见本 object 的文档注释；一切失败 = `router.invalid_args`
   * + `details.reason`（`json_exclusive` / `json_not_object` / `unknown_flag` /
   * `flag_missing_value` / `duplicate_flag` / `too_many_positionals` / `positional_flag_conflict`）。
   */
  def bind(cmd: CommandDef, argv: List[String]): Either[IrError, JsonObject] =
    val byName = cmd.params.map(p => p.name -> p).toMap

    /** 左到右单遍。`--json` 是保留 token（[L4]）：永不作 `--key` 的值或 positional。 */
    def loop(
      rest: List[String],
      assigned: Vector[(String, Json)],
      positional: List[String],
      json: Option[JsonObject]
    ): Either[IrError, JsonObject] = rest match
      case Nil =>
        // --json 与任何其它实参互斥（§9:543 / C24）
        if json.isDefined && (assigned.nonEmpty || positional.nonEmpty) then Left(jsonExclusive())
        else if json.isDefined then Right(json.get)
        else bindPositional(cmd, assigned, positional.reverse)
      case "--json" :: value :: tail if json.isEmpty =>
        parseJsonObject(value) match
          case Left(err) => Left(err)
          case Right(obj) => loop(tail, assigned, positional, Some(obj))
      case "--json" :: Nil =>
        // 尾随 --json 无值：flag 缺值的糖面镜像（非对象值归 json_not_object，缺值归本档）
        Left(flagMissingValue("--json"))
      case "--json" :: _ =>
        // 第二次出现（已有一个 --json）⇒ 与其它实参并存
        Left(jsonExclusive())
      case tok :: tail if tok.startsWith("--") =>
        val key = tok.drop(2)
        byName.get(key) match
          case None => Left(unknownFlag(tok))
          case Some(p) =>
            if assigned.exists(_._1 == key) then Left(duplicateFlag(key))
            else if p.isFlag then loop(tail, assigned :+ (key -> true.asJson), positional, json)
            else
              tail match
                case value :: more if !value.startsWith("--") =>
                  loop(more, assigned :+ (key -> value.asJson), positional, json)
                case _ => Left(flagMissingValue(tok))
      case tok :: tail =>
        loop(tail, assigned, tok :: positional, json)

    loop(argv, Vector.empty, Nil, None)
  end bind

  /** positional：token i → 第 i 个槽位（按 `Param.positional` 升序；注册期保证 0..n-1 无跳号）。 */
  private def bindPositional(
    cmd: CommandDef,
    assigned: Vector[(String, Json)],
    tokens: List[String]
  ): Either[IrError, JsonObject] =
    val slots = cmd.params.filter(_.positional.isDefined).sortBy(_.positional.get)
    if tokens.length > slots.length then
      Left(
        IrError
          .invalidArgs(s"too many positional arguments (${tokens.length} for ${slots.length} slot(s))")
          .withDetail("reason", "too_many_positionals".asJson)
          .withDetail("got", tokens.length.asJson)
          .withDetail("slots", slots.length.asJson)
      )
    else
      val conflicts = tokens
        .zip(slots)
        .collect { case (_, slot) if assigned.exists(_._1 == slot.name) => slot.name }
        .distinct
      if conflicts.nonEmpty then
        Left(
          IrError
            .invalidArgs(s"argument(s) set both positionally and by flag: ${conflicts.mkString(", ")}")
            .withDetail("reason", "positional_flag_conflict".asJson)
            .withDetail("args", conflicts.asJson)
        )
      else
        // default 不注入：只有显式绑定的键出现在 args 里（[D19]：缺省 ≠ 显式 "."）
        Right(JsonObject.fromIterable(assigned ++ tokens.zip(slots).map((t, s) => s.name -> t.asJson)))

    end if

  end bindPositional

  private def parseJsonObject(raw: String): Either[IrError, JsonObject] =
    parser.parse(raw) match
      case Left(_) =>
        Left(
          IrError
            .invalidArgs(s"--json value is not valid JSON (P1 sugar takes one compact token)")
            .withDetail(
              "reason",
              "json_not_object".asJson
            )
        )
      case Right(j) =>
        j.asObject match
          case Some(o) => Right(o)
          case None =>
            Left(
              IrError
                .invalidArgs("--json value must be a JSON object")
                .withDetail(
                  "reason",
                  "json_not_object".asJson
                )
            )

  private def jsonExclusive(): IrError =
    IrError
      .invalidArgs("--json cannot be combined with any other argument (the JSON escape hatch is exclusive)")
      .withDetail("reason", "json_exclusive".asJson)

  private def unknownFlag(tok: String): IrError =
    IrError
      .invalidArgs(s"unknown flag '$tok' (flags must be declared in the command's params)")
      .withDetail("reason", "unknown_flag".asJson)
      .withDetail("flag", tok.asJson)

  private def flagMissingValue(tok: String): IrError =
    IrError
      .invalidArgs(s"flag '$tok' needs a value (a following token starting with '--' is not consumed as a value)")
      .withDetail("reason", "flag_missing_value".asJson)
      .withDetail("flag", tok.asJson)

  private def duplicateFlag(key: String): IrError =
    IrError
      .invalidArgs(s"flag '--$key' given more than once")
      .withDetail(
        "reason",
        "duplicate_flag".asJson
      )
      .withDetail("flag", key.asJson)

  // ── lower 总装 ────────────────────────────────────────────

  /**
   * `/<name> [argv…]` → `Call(name, args)`。顺序：首 token 去 `/` 得 name →
   * `Names.syntax`（R3/N1/N2：语法坏名 ⇒ `router.schema.name_invalid`）→ 查表（语法合法
   * 但未注册 ⇒ `router.unknown_command`，C3/§6.2——与直连 `ir` 腿同码）→ [[bind]]。
   *
   * gateway 侧的成形出口 = `Router.invalidResult`（exit 2 / 127，§6.4）；本函数只产出
   * [[IrError]]，不碰退出码。
   */
  def lower(content: String, lookup: String => Option[CommandDef]): Either[IrError, Ir.Call] =
    tokenize(content) match
      case Nil => Left(IrError.nameInvalid(content.trim, "empty command line"))
      case head :: tail =>
        val name = head.drop(1) // 去前导 /
        Names.syntax(name).flatMap { _ =>
          lookup(name) match
            case None => Left(IrError.unknownCommand(name))
            case Some(cmd) => bind(cmd, tail).map(args => Ir.Call(name, args))
        }

end ArgvSugar
