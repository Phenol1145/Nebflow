package nebflow.ir

import io.circe.Json
import io.circe.syntax.*
import munit.CatsEffectSuite

/**
 * 线格式与守卫的一致性用例（`.zcode/plans/command-ir-standard.md` §11）：
 * C4/C5/C10/C14/C15/C16/C20/C22/C26/C27/C28/C29/C32/C40 与 §3.3/§10.1 的其余 MUST。
 * 全部走 **JSON 线格式**（不经 Scala 形状），因为标准说「线格式是规范」。
 */
class IrCodecSpec extends CatsEffectSuite:

  private def codeOf(text: String): Option[String] =
    Ir.parse(text).left.toOption.map(_.code)

  /** 解码 + 树形校验（§3.3/§5.5 的 R6/R9/S6 在 [[Ir.validate]]，不在解码里）。 */
  private def validatedCodeOf(text: String): Option[String] =
    Ir.parse(text)
      .flatMap(ir => Ir.validate(ir, IrLimits.default))
      .left
      .toOption
      .map(_.code)

  private def nodeTag(text: String): Option[String] =
    Ir.parse(text).left.toOption.flatMap(_.details("key").flatMap(_.asString))

  // ── §3.2/§3.3 节点文法 ────────────────────────────────────

  test("C4 未知键 ⇒ router.schema.unknown_key"):
    assertEquals(codeOf("""{"call":{"name":"dev:fs:ls","argz":{}}}"""), Some(Codes.SchemaUnknownKey))

  test("C5 双标签 ⇒ router.schema.node_tag"):
    assertEquals(
      codeOf("""{"call":{"name":"dev:fs:ls"},"pipe":{"stages":[]}}"""),
      Some(Codes.SchemaNodeTag)
    )

  test("C10/R10 空计划 / null / 非对象根 ⇒ router.schema.node_tag"):
    assertEquals(codeOf("{}"), Some(Codes.SchemaNodeTag))
    assertEquals(codeOf("null"), Some(Codes.SchemaNodeTag))
    assertEquals(codeOf("[]"), Some(Codes.SchemaNodeTag))
    assertEquals(codeOf("""{"sequence":{"items":[]}}"""), Some(Codes.InvalidArgs))

  test("C26 保留字段（v1 占名）⇒ router.schema.unknown_key，细节带键名"):
    assertEquals(codeOf("""{"call":{"name":"dev:fs:ls","cwd":"."}}"""), Some(Codes.SchemaUnknownKey))
    assertEquals(nodeTag("""{"call":{"name":"dev:fs:ls","cwd":"."}}"""), Some("cwd"))
    assertEquals(codeOf("""{"call":{"name":"dev:fs:ls","timeoutMs":1}}"""), Some(Codes.SchemaUnknownKey))

  test("C27 名称语法：大写与缺段都 ⇒ router.schema.name_invalid"):
    assertEquals(codeOf("""{"call":{"name":"Dev:Fs:LS"}}"""), Some(Codes.SchemaNameInvalid))
    assertEquals(codeOf("""{"call":{"name":"dev"}}"""), Some(Codes.SchemaNameInvalid))
    assertEquals(codeOf("""{"call":{"name":"dev:fs:ls","args":[]}}"""), Some(Codes.InvalidArgs))

  test("R5 stdin 字面量：未知内键 ⇒ unknown_key；双键 ⇒ invalid_args"):
    assertEquals(codeOf("""{"call":{"name":"dev:fs:cat","stdin":{"junk":1}}}"""), Some(Codes.SchemaUnknownKey))
    assertEquals(codeOf("""{"call":{"name":"dev:fs:cat","stdin":{"text":"a","jsonl":[]}}}"""), Some(Codes.InvalidArgs))

  test("R5/R4 stdin 与 args 的形状必须合法"):
    assertEquals(codeOf("""{"call":{"name":"dev:fs:cat","stdin":"x"}}"""), Some(Codes.InvalidArgs))
    assertEquals(codeOf("""{"call":{"name":"dev:fs:cat","stdin":{"text":7}}}"""), Some(Codes.InvalidArgs))

  test("R7 管道至少两级 / 顺序至少一项"):
    assertEquals(codeOf("""{"pipe":{"stages":[{"call":{"name":"dev:fs:ls"}}]}}"""), Some(Codes.InvalidArgs))

  test("R8 redirect.op 未知值 ⇒ 严格拒绝（v1: in|out|append）"):
    assertEquals(
      codeOf("""{"redirect":{"op":"zap","path":"x","inner":{"call":{"name":"dev:fs:ls"}}}}"""),
      Some(Codes.SchemaBadValue)
    )

  test("C28 规模上限：节点 65 ⇒ router.limit.nodes；深度 9 ⇒ router.limit.depth"):
    val sixtysix = (1 to 66).map(_ => """{"call":{"name":"dev:fs:ls"}}""").mkString(",")
    assertEquals(validatedCodeOf(s"""{"pipe":{"stages":[$sixtysix]}}"""), Some(Codes.LimitNodes))
    // 7 层 redirect = 深度 8（上限内）；8 层 = 深度 9 ⇒ 超限
    val deep7 = (1 to 7).foldLeft("""{"call":{"name":"dev:fs:ls"}}""")((inner, _) =>
      s"""{"redirect":{"op":"out","path":"x","inner":$inner}}"""
    )
    assertEquals(Ir.parse(deep7).flatMap(ir => Ir.validate(ir, IrLimits.default)).isRight, true)
    val deep8 = s"""{"redirect":{"op":"out","path":"x","inner":$deep7}}"""
    assertEquals(validatedCodeOf(deep8), Some(Codes.LimitDepth))

  test("C32 redirect 形态：包 Sequence / 作管道非末级 ⇒ router.schema.bad_value"):
    assertEquals(
      validatedCodeOf(
        """{"redirect":{"op":"out","path":"x","inner":{"sequence":{"items":[{"call":{"name":"dev:fs:ls"}}]}}}}"""
      ),
      Some(Codes.SchemaBadValue)
    )
    assertEquals(
      validatedCodeOf(
        """{"pipe":{"stages":[{"redirect":{"op":"out","path":"x","inner":{"call":{"name":"dev:fs:ls"}}}},{"call":{"name":"dev:fs:cat"}}]}}"""
      ),
      Some(Codes.SchemaBadValue)
    )

  test("C6 stdin 冲突：非首级 stage 自带 stdin ⇒ router.invalid_args（reason=stdin_conflict）"):
    val json =
      """{"pipe":{"stages":[{"call":{"name":"dev:fs:ls"}},{"call":{"name":"dev:fs:cat","stdin":{"text":"x"}}}]}}"""
    val err = Ir.parse(json).flatMap(ir => Ir.validate(ir, IrLimits.default)).left.toOption
    assertEquals(err.map(_.code), Some(Codes.InvalidArgs))
    assertEquals(err.flatMap(_.details("reason").flatMap(_.asString)), Some("stdin_conflict"))

  // ── §0 信封 ───────────────────────────────────────────────

  test("C20 信封 ir 版本不支持 ⇒ router.schema.bad_version"):
    val env = Json
      .obj(
        "ir" -> 2.asJson,
        "plan" -> Ir.encode(Ir.Call("dev:fs:ls", io.circe.JsonObject.empty)),
        "tenant" -> Tenant.toJson(Tenant.Human("local")),
        "ingress" -> "human".asJson,
        "sessionId" -> "s".asJson,
        "requestId" -> "r".asJson
      )
    assertEquals(IrRequest.decode(env).left.toOption.map(_.code), Some(Codes.SchemaBadVersion))

  test("C40 信封 ↔ 租户一致性：human 租户配 llm ingress ⇒ router.schema.bad_value"):
    val env = IrTestKit.envelopeJson(Ir.Call("dev:fs:ls", io.circe.JsonObject.empty), ingress = Ingress.Llm)
    assertEquals(IrRequest.decode(env).left.toOption.map(_.code), Some(Codes.SchemaBadValue))
    // llm 租户配 script ingress 合法（脚本由谁触发不改变权限主体）
    val ok = IrTestKit.envelopeJson(
      Ir.Call("dev:fs:ls", io.circe.JsonObject.empty),
      tenant = Tenant.Llm("sess", "Nebula"),
      ingress = Ingress.Script
    )
    assertEquals(IrRequest.decode(ok).isRight, true)

  test("信封未知键 ⇒ router.schema.unknown_key；缺键 ⇒ router.invalid_args"):
    val base = IrTestKit.envelopeJson(Ir.Call("dev:fs:ls", io.circe.JsonObject.empty))
    assertEquals(
      IrRequest.decode(base.deepMerge(Json.obj("extra" -> 1.asJson))).left.toOption.map(_.code),
      Some(Codes.SchemaUnknownKey)
    )
    assertEquals(
      IrRequest.decode(base.deepMerge(Json.obj("sessionId" -> "".asJson))).left.toOption.map(_.code),
      Some(Codes.InvalidArgs)
    )

  // ── §4.6 路径值域 ─────────────────────────────────────────

  test("C14 越出 VFS 根 ⇒ router.schema.bad_value（计划非法，不是策略拒绝）"):
    assertEquals(VfsPath.canon("../../etc/passwd").left.toOption.map(_.code), Some(Codes.SchemaBadValue))
    assertEquals(VfsPath.canon("a/../../b").left.toOption.map(_.code), Some(Codes.SchemaBadValue))

  test("C15 `~` 在 IR 层永不展开 ⇒ router.schema.bad_value"):
    assertEquals(VfsPath.canon("~/x").left.toOption.map(_.code), Some(Codes.SchemaBadValue))
    assertEquals(VfsPath.canon("a/~/b").left.toOption.map(_.code), Some(Codes.SchemaBadValue))

  test("C16 路径 canonical：a/../b 与 b 判定一致"):
    assertEquals(VfsPath.canon("a/../b"), VfsPath.canon("b"))
    assertEquals(VfsPath.canon("./x/"), VfsPath.canon("x"))
    assertEquals(VfsPath.canon("/home/x"), VfsPath.canon("home/x")) // [P1] `/` = VFS 根

  test("P4 宿主形态必须拒绝：Windows 盘符与 UNC；P5 反斜杠规范化"):
    assertEquals(VfsPath.canon("""C:\Users\x\.ssh\id_rsa""").left.toOption.map(_.code), Some(Codes.SchemaBadValue))
    assertEquals(VfsPath.canon("""\\server\share\f""").left.toOption.map(_.code), Some(Codes.SchemaBadValue))
    assertEquals(VfsPath.canon("""a\b"""), VfsPath.canon("a/b"))

  test("P2/P6 cap 前缀按段比较：前缀边界不被伪匹配"):
    val work = VfsPath.canonPrefix("work").toOption.get
    assertEquals(VfsPath.isWithin(work, VfsPath.canon("work/a").toOption.get), true)
    assertEquals(VfsPath.isWithin(work, VfsPath.canon("work").toOption.get), true)
    assertEquals(VfsPath.isWithin(work, VfsPath.canon("workshop/a").toOption.get), false)
    assertEquals(VfsPath.isWithin(VfsPath.Canon(Nil), VfsPath.canon("any/where").toOption.get), true)

  // ── §4.1/§4.5 摘要 ────────────────────────────────────────

  test("C22 摘要稳定性：键序无关；等价数值同摘要"):
    val a = io.circe.parser.parse("""{"b":1,"a":{"d":[1,2],"c":true}}""").toOption.get
    val b = io.circe.parser.parse("""{"a":{"c":true,"d":[1,2]},"b":1}""").toOption.get
    assertEquals(Canonical.digest(a), Canonical.digest(b))
    val n1 = io.circe.parser.parse("""{"a":1.0}""").toOption.get
    val n2 = io.circe.parser.parse("""{"a":1}""").toOption.get
    assertEquals(Canonical.digest(n1), Canonical.digest(n2))

  test("§4.1 打印器：排序 + 紧凑 + 最短往返（禁 circe 默认打印器）"):
    val json = io.circe.parser.parse("""{"b":[1.5,2e2],"a":"x"}""").toOption.get
    assertEquals(Canonical.print(json), """{"a":"x","b":[1.5,200]}""")

end IrCodecSpec
