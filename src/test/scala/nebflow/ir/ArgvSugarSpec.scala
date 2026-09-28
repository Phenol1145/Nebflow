package nebflow.ir

import cats.effect.IO
import io.circe.Json
import io.circe.syntax.*
import munit.CatsEffectSuite

/**
 * 人侧 argv 糖（P1-1，标准 §9 ingress #1）的一致性用例：humanShape 分流真值表、
 * tokenize、bind 全矩阵、lower 端到端、DevCommands params 锚（本批新增声明）、
 * Registry 两条注册期新守卫、`Router.invalidResult` 的 §6.4 退出码映射。
 *
 * 纯函数面（零 IO 执行）：audience/路径/策略校验不在糖层（Planner/Executor 双检查点），
 * 故本 Spec 不起 VFS——只有 DevCommands 锚与 Registry 守卫触碰真实描述符。
 */
class ArgvSugarSpec extends CatsEffectSuite:

  private val devHandler: DevHandler = (_, _) => IO.pure(Right(StreamValue.Text("")))

  /** 双形态命令：path（值参 + positional 0）与 verbose（isFlag）。 */
  private val sugarCmd: CommandDef =
    CommandDef(
      name = "dev:test:sugar",
      description = "argv sugar fixture (test only)",
      argsSchema = Json
        .obj(
          "type" -> "object".asJson,
          "properties" -> Json.obj(
            "path" -> Json.obj("type" -> "string".asJson),
            "verbose" -> Json.obj("type" -> "boolean".asJson)
          ),
          "additionalProperties" -> false.asJson
        )
        .asObject
        .get,
      binding = Binding.Dev(devHandler),
      io = CommandIo(stdin = None, stdout = StreamKind.Text),
      params = List(Param("path", positional = Some(0)), Param("verbose", isFlag = true))
    )

  /** 双槽命令：positional 0/1。 */
  private val twoSlots: CommandDef = sugarCmd.copy(
    name = "dev:test:slots",
    params = List(Param("a", positional = Some(0)), Param("b", positional = Some(1)))
  )

  private def reason(err: IrError): Option[String] = err.details("reason").flatMap(_.asString)

  private def bindErr(cmd: CommandDef, argv: String*): (String, Option[String]) =
    val e = ArgvSugar.bind(cmd, argv.toList).left.getOrElse(fail("expected Left"))
    (e.code, reason(e))

  private def registryOf(cmds: CommandDef*): CommandRegistry =
    val r = new CommandRegistry()
    cmds.foreach(c => r.register(c).left.foreach(e => fail(s"fixture registration failed: ${e.message}")))
    r

  // ── ① humanShape 分流真值表（§9:547 + 三颗优先序钉子） ────

  test("① humanShape：白名单正向判定（首段 ∈ 四值 ∧ ≥2 段）"):
    for text <- List(
        "/dev:fs:ls",
        " /dev:fs:ls",
        "/dev:fs:ls --path .",
        "\t/dev:fs:ls",
        "/mcp:s:t",
        "/ext:x:y",
        "/bash:x"
      )
    do assertEquals(ArgvSugar.humanShape(text), true, s"expected sugar shape: '$text'")

  test("① humanShape：保留名集合 = 白名单的补（/clear 族不 lower，无需另维护清单）"):
    for text <- List("/clear", "/compact", "/ask", "/onboarding", "/skill:x", "/skill")
    do assertEquals(ArgvSugar.humanShape(text), false, s"reserved slash name must not lower: '$text'")

  test("① humanShape：// 转义 / 单段 / 首段越界 / 大小写 / 空串"):
    // // 转义（InputMentions 的边界，显式排除）
    assertEquals(ArgvSugar.humanShape("//dev:fs:ls"), false)
    // 单段 / 首段 ∉ 四值
    assertEquals(ArgvSugar.humanShape("/dev"), false)
    assertEquals(ArgvSugar.humanShape("/devfoo:x"), false)
    // N2：禁大小写折叠
    assertEquals(ArgvSugar.humanShape("/DEV:fs:ls"), false)
    assertEquals(ArgvSugar.humanShape("/Dev:fs:ls"), false)
    // 空串 / 全空白 / 非斜杠起头
    assertEquals(ArgvSugar.humanShape(""), false)
    assertEquals(ArgvSugar.humanShape("   "), false)
    assertEquals(ArgvSugar.humanShape("hello world"), false)

  test("① humanShape：语法坏名仍 lower（两档错误在 lower 分层，不在分流闸）"):
    // /dev:FS:ls 首段合法、两段 ⇒ humanShape=true：段内大写归 lower 报 name_invalid（见 ④）
    assertEquals(ArgvSugar.humanShape("/dev:FS:ls"), true)

  // ── ② tokenize ───────────────────────────────────────────

  test("② tokenize：ASCII 空白切分 + 连续折叠；空 ⇒ Nil"):
    assertEquals(ArgvSugar.tokenize("a  b"), List("a", "b"))
    assertEquals(ArgvSugar.tokenize("a\t\nb"), List("a", "b"))
    assertEquals(ArgvSugar.tokenize("  a\t b  \r\n"), List("a", "b"))
    assertEquals(ArgvSugar.tokenize(""), Nil)
    assertEquals(ArgvSugar.tokenize("   "), Nil)

  test("② tokenize：引号是普通 token 字符（P1 无引号/无转义，§9:549 冻结 P2）"):
    assertEquals(ArgvSugar.tokenize("\"a b\""), List("\"a", "b\""))

  test("② tokenize：多行 content 整体当一个命令行（换行后的词成为 argv）"):
    assertEquals(ArgvSugar.tokenize("/dev:fs:ls\nextra"), List("/dev:fs:ls", "extra"))

  // ── ③ bind 全矩阵 ────────────────────────────────────────

  test("③ bind：--key value / isFlag 不消费下一 token"):
    assertEquals(
      ArgvSugar.bind(sugarCmd, List("--path", "x")).getOrElse(fail("bind failed")),
      Json.obj("path" -> "x".asJson).asObject.get
    )
    assertEquals(
      ArgvSugar.bind(sugarCmd, List("--verbose")).getOrElse(fail("bind failed")),
      Json.obj("verbose" -> true.asJson).asObject.get
    )
    // isFlag 之后跟的 token 落 positional，不被吞为值
    assertEquals(
      ArgvSugar.bind(sugarCmd, List("--verbose", ".")).getOrElse(fail("bind failed")),
      Json.obj("verbose" -> true.asJson, "path" -> ".".asJson).asObject.get
    )

  test("③ bind：--key 缺值 / 下一 token 以 -- 起头 ⇒ flag_missing_value"):
    assertEquals(bindErr(sugarCmd, "--path"), (Codes.InvalidArgs, Some("flag_missing_value")))
    assertEquals(bindErr(sugarCmd, "--path", "--json"), (Codes.InvalidArgs, Some("flag_missing_value")))
    assertEquals(
      bindErr(sugarCmd, "--verbose", "--path", "x", "--verbose"),
      (Codes.InvalidArgs, Some("duplicate_flag"))
    )

  test("③ bind：未知 flag / 裸 -- / 重复 flag"):
    assertEquals(bindErr(sugarCmd, "--nope", "x"), (Codes.InvalidArgs, Some("unknown_flag")))
    assertEquals(bindErr(sugarCmd, "--"), (Codes.InvalidArgs, Some("unknown_flag")))
    assertEquals(bindErr(sugarCmd, "--path", "a", "--path", "b"), (Codes.InvalidArgs, Some("duplicate_flag")))

  test("③ bind：positional 按 slot 序绑 / 超额 / 与 flag 同键冲突"):
    assertEquals(
      ArgvSugar.bind(twoSlots, List("x", "y")).getOrElse(fail("bind failed")),
      Json.obj("a" -> "x".asJson, "b" -> "y".asJson).asObject.get
    )
    assertEquals(bindErr(twoSlots, "x", "y", "z"), (Codes.InvalidArgs, Some("too_many_positionals")))
    assertEquals(bindErr(sugarCmd, "--path", "x", "."), (Codes.InvalidArgs, Some("positional_flag_conflict")))
    // 单 dash token 是 positional（无短 flag）
    assertEquals(
      ArgvSugar.bind(sugarCmd, List("-x")).getOrElse(fail("bind failed")),
      Json.obj("path" -> "-x".asJson).asObject.get
    )

  test("③ bind：--json 逃生口独占（C24 糖面镜像）"):
    assertEquals(
      ArgvSugar.bind(sugarCmd, List("--json", "{}")).getOrElse(fail("bind failed")),
      Json.obj().asObject.get
    )
    assertEquals(
      ArgvSugar.bind(sugarCmd, List("--json", """{"a":1,"b":"x"}""")).getOrElse(fail("bind failed")),
      Json.obj("a" -> 1.asJson, "b" -> "x".asJson).asObject.get
    )
    assertEquals(bindErr(sugarCmd, "--json", "{}", "--path", "x"), (Codes.InvalidArgs, Some("json_exclusive")))
    assertEquals(bindErr(sugarCmd, "--json", "{}", "."), (Codes.InvalidArgs, Some("json_exclusive")))
    assertEquals(bindErr(sugarCmd, "--path", "x", "--json", "{}"), (Codes.InvalidArgs, Some("json_exclusive")))
    // 非对象 / 非法 JSON ⇒ invalid_args（json_not_object）
    assertEquals(bindErr(sugarCmd, "--json", "[]"), (Codes.InvalidArgs, Some("json_not_object")))
    assertEquals(bindErr(sugarCmd, "--json", "nope"), (Codes.InvalidArgs, Some("json_not_object")))
    assertEquals(bindErr(sugarCmd, "--json"), (Codes.InvalidArgs, Some("flag_missing_value")))

  test("③ bind：default 不注入（缺省 ⇒ 键缺席，[D19]）"):
    val args = ArgvSugar.bind(sugarCmd, Nil).getOrElse(fail("bind failed"))
    assert(!args.contains("path"), "default 必须不注入：空 argv ⇒ args 无 path 键")
    assert(!args.contains("verbose"))

  // ── ④ lower 端到端 ───────────────────────────────────────

  test("④ lower：/dev:fs:ls → Call(dev:fs:ls,{})；--path 双形态"):
    val reg = registryOf(DevCommands.base*)
    val call1 = ArgvSugar.lower("/dev:fs:ls", reg.get).getOrElse(fail("lower failed"))
    assertEquals(call1.name, "dev:fs:ls")
    assertEquals(call1.args, io.circe.JsonObject())
    val call2 = ArgvSugar.lower("/dev:fs:ls --path .", reg.get).getOrElse(fail("lower failed"))
    assertEquals(call2.args("path"), Some(".".asJson))

  test("④ lower：语法坏名 ⇒ name_invalid；未注册 ⇒ unknown_command（与直连 ir 腿同码）"):
    val reg = registryOf(DevCommands.base*)
    val badName = ArgvSugar.lower("/dev:FS:ls", reg.get).left.getOrElse(fail("expected Left"))
    assertEquals(badName.code, Codes.SchemaNameInvalid)
    assertEquals(badName.details("name"), Some("/dev:FS:ls".drop(1).asJson))
    val unknown = ArgvSugar.lower("/dev:fs:nope", reg.get).left.getOrElse(fail("expected Left"))
    assertEquals(unknown.code, Codes.UnknownCommand)

  test("④ lower：mcp 面 params 恒空 ⇒ 一切 --key 都是 unknown_flag，外部面只有 --json"):
    val mcpCmd = CommandDef(
      name = "mcp:test:t",
      description = "foreign fixture (test only)",
      argsSchema = Json.obj("type" -> "object".asJson).asObject.get,
      binding = Binding.Mcp("test", "t"),
      io = CommandIo(stdin = None, stdout = StreamKind.Text)
    )
    val reg = registryOf(mcpCmd)
    assertEquals(bindErr(mcpCmd, "--key", "k", "v")._2, Some("unknown_flag"))
    val viaLower = ArgvSugar.lower("""/mcp:test:t --key k v""", reg.get).left.getOrElse(fail("expected Left"))
    assertEquals(viaLower.code, Codes.InvalidArgs)
    assertEquals(reason(viaLower), Some("unknown_flag"))
    val ok = ArgvSugar.lower("""/mcp:test:t --json {"a":1}""", reg.get).getOrElse(fail("lower failed"))
    assertEquals(ok.args("a"), Some(1.asJson))

  // ── ⑤ DevCommands params 锚（本批新增声明） ───────────────

  test("⑤ DevCommands.base 全部经新 Registry 守卫注册成功"):
    val r = new CommandRegistry()
    DevCommands.base.foreach { c =>
      assertEquals(r.register(c).isRight, true, s"${c.name} 必须可注册")
    }

  test("⑤ fsLs/fsCat 的 params == List(Param(path, positional=Some(0), required=false))"):
    val expected = List(Param("path", positional = Some(0), required = false))
    assertEquals(DevCommands.fsLs.params, expected)
    assertEquals(DevCommands.fsCat.params, expected)

  test("⑤ 真 fsLs bind：positional 与 flag 双形态同结果；空 argv ⇒ 键缺席"):
    assertEquals(
      ArgvSugar.bind(DevCommands.fsLs, List(".")).getOrElse(fail("bind failed")),
      Json.obj("path" -> ".".asJson).asObject.get
    )
    assertEquals(
      ArgvSugar.bind(DevCommands.fsLs, List("--path", ".")).getOrElse(fail("bind failed")),
      Json.obj("path" -> ".".asJson).asObject.get
    )
    assert(!ArgvSugar.bind(DevCommands.fsLs, Nil).getOrElse(fail("bind failed")).contains("path"))
    // fsCat 同验：不带参 ⇒ 读 stdin 语义不变（args 无 path 键）
    assert(!ArgvSugar.bind(DevCommands.fsCat, Nil).getOrElse(fail("bind failed")).contains("path"))
    assertEquals(
      ArgvSugar.bind(DevCommands.fsCat, List("a.txt")).getOrElse(fail("bind failed"))("path"),
      Some("a.txt".asJson)
    )

  // ── ⑥ Registry 两条注册期新守卫 ──────────────────────────

  test("⑥ 守卫①：Mcp/Node binding 且 params 非空 ⇒ 注册失败（params_foreign_empty）"):
    val mcpWithParams = CommandDef(
      name = "mcp:test:p",
      description = "foreign fixture with params (test only)",
      argsSchema = Json
        .obj("type" -> "object".asJson, "properties" -> Json.obj("k" -> Json.obj("type" -> "string".asJson)))
        .asObject
        .get,
      binding = Binding.Mcp("test", "p"),
      io = CommandIo(stdin = None, stdout = StreamKind.Text),
      params = List(Param("k"))
    )
    val nodeWithParams = mcpWithParams.copy(name = "ext:test:p", binding = Binding.Node("test", "p"))
    for cmd <- List(mcpWithParams, nodeWithParams) do
      new CommandRegistry().register(cmd) match
        case Left(err) =>
          assertEquals(err.code, Codes.InvalidArgs)
          assertEquals(reason(err), Some("params_foreign_empty"))
        case Right(_) => fail(s"${cmd.name} 带 params 的外部绑定必须注册失败")

  test("⑥ 守卫②：positional 槽位不得跳号（(1,2) 拒；(0,1) 过；重复拒）"):
    def devParams(params: List[Param]): CommandDef =
      val props = params.map(p => p.name -> Json.obj("type" -> "string".asJson))
      sugarCmd.copy(
        name = "dev:test:gapped",
        params = params,
        argsSchema = Json.obj("type" -> "object".asJson, "properties" -> Json.obj(props*)).asObject.get
      )
    val gapped = devParams(List(Param("a", positional = Some(1)), Param("b", positional = Some(2))))
    new CommandRegistry().register(gapped) match
      case Left(err) => assertEquals(reason(err), Some("positional_gap"))
      case Right(_) => fail("跳号的 positional 必须注册失败")
    val contiguous = devParams(List(Param("a", positional = Some(0)), Param("b", positional = Some(1))))
    assertEquals(new CommandRegistry().register(contiguous).isRight, true)
    val duplicated = devParams(List(Param("a", positional = Some(0)), Param("b", positional = Some(0))))
    new CommandRegistry().register(duplicated) match
      case Left(err) => assertEquals(err.code, Codes.InvalidArgs)
      case Right(_) => fail("重复槽位必须注册失败")

  // ── ⑦ Router.invalidResult 的 §6.4 退出码映射 ─────────────

  test("⑦ invalidResult：name_invalid→2、invalid_args→2、unknown_command→127，status 均 invalid"):
    val router = new Router(new CommandRegistry(), new PolicyEngine(PolicyConfig()), AuditSink.noop)
    def check(err: IrError, expectedExit: Int): Unit =
      val r = router.invalidResult("req-sugar", err)
      assertEquals(r.status, Status.Invalid)
      assertEquals(r.exit, Some(expectedExit))
      assertEquals(r.error.map(_.code), Some(err.code))
      assertEquals(r.requestId, "req-sugar")
    check(IrError.nameInvalid("/dev:FS:ls".drop(1), "segment #1 is not [a-z0-9][a-z0-9_-]*"), ExitCode.Usage)
    check(IrError.invalidArgs("--json cannot be combined with any other argument"), ExitCode.Usage)
    check(IrError.unknownCommand("dev:fs:nope"), ExitCode.UnknownCommand)

end ArgvSugarSpec
