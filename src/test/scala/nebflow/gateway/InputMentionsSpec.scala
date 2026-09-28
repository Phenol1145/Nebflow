// mention-tokens(feat/mention-tokens 2026-09-27):InputMentions 分词/解析/指针块规格。
// 纯逻辑用 stub Lookups;fileLookup 用真实临时目录。fail-open 与文本逐字节不变是核心契约。
package nebflow.gateway

import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite

import java.nio.file.Files

class InputMentionsSpec extends CatsEffectSuite:

  private val noneLookups = InputMentions.Lookups(
    file = _ => IO.pure(None),
    project = _ => IO.pure(None),
    flow = _ => IO.pure(None),
    session = _ => IO.pure(None),
    skill = _ => IO.pure(None)
  )

  private def lookupsWith(
    project: Map[String, (String, String)] = Map.empty,
    skill: Map[String, (String, String)] = Map.empty
  ): InputMentions.Lookups =
    InputMentions.Lookups(
      file = _ => IO.pure(None),
      project = name => IO.pure(project.get(name.toLowerCase)),
      flow = _ => IO.pure(None),
      session = _ => IO.pure(None),
      skill = name => IO.pure(skill.get(name))
    )

  test("无提及文本逐字节原样"):
    val t = "普通一句话，不带任何提及。"
    InputMentions.resolve(t, noneLookups).map { case (out, unres) =>
      assertEquals(out, t)
      assertEquals(unres, List.empty[InputMentions.Mention])
    }

  test("分词：CJK 紧邻可识别，邮箱与裸 @词 不命中"):
    val ms = InputMentions.tokenize("看@project:nebflow 的进展，联系 user@x.com，在 @example 找 @src/main/core")
    assertEquals(ms.collect { case p: InputMentions.ProjectMention => p.name }, List("nebflow"))
    assertEquals(ms.collect { case f: InputMentions.FileMention => f.path }, List("src/main/core"))

  test("分词：尾随标点剥离"):
    val ms = InputMentions.tokenize("总结 @project:demo。然后 @skill:review-skill！")
    assertEquals(ms.collect { case p: InputMentions.ProjectMention => p.name }, List("demo"))
    assertEquals(ms.collect { case s: InputMentions.SkillMention => s.name }, List("review-skill"))

  test("分词：CJK 句读截断（无空格中文文本不吞句）"):
    val ms = InputMentions.tokenize("压缩@project:demo。然后看 @skill:review-skill！收尾 @src/a.scala，完")
    assertEquals(ms.collect { case p: InputMentions.ProjectMention => p.name }, List("demo"))
    assertEquals(ms.collect { case s: InputMentions.SkillMention => s.name }, List("review-skill"))
    assertEquals(ms.collect { case f: InputMentions.FileMention => f.path }, List("src/a.scala"))

  test("分词：反斜杠转义 \\@ 不解析；$ 已退役全惰性"):
    assertEquals(InputMentions.tokenize("这是 \\@project:x"), List.empty[InputMentions.Mention])
    assertEquals(InputMentions.tokenize("用 $review 和 $code/review 走查"), List.empty[InputMentions.Mention])

  test("分词：@@ 转义不解析（双 @ 起首的 token 整体不产生提及）"):
    assertEquals(InputMentions.tokenize("看 @@project:x 和 @@src/file"), List.empty[InputMentions.Mention])

  test("分词：@skill: 支持命名空间形态"):
    val ms = InputMentions.tokenize("用 @skill:code/review 走查")
    assertEquals(ms.collect { case s: InputMentions.SkillMention => s.name }, List("code/review"))

  test("resolve：指针块追加尾部并按指针行去重"):
    val lk = lookupsWith(
      project = Map("nebflow" -> (("Nebflow", "/ws/nebflow"))),
      skill = Map("review" -> (("review", "审阅技能")))
    )
    val t = "总结 @project:nebflow 与 @project:nebflow 的差异，用 @skill:review 走查"
    InputMentions.resolve(t, lk).map { case (out, unres) =>
      assertEquals(unres, List.empty[InputMentions.Mention])
      assertEquals(
        out.drop(t.length),
        "\n\n[提及解析]\n[引用: 项目 · Nebflow · /ws/nebflow]\n[技能: review — 审阅技能]"
      )
    }

  test("resolve：@@ 转义还原为单 @ 且零指针（含与真提及共存）"):
    val lk = lookupsWith(project = Map("demo" -> (("demo", "/ws/demo"))))
    val t = "看 @@types/node 包和 @project:demo"
    val reduced = "看 @types/node 包和 @project:demo"
    InputMentions.resolve(t, lk).map { case (out, unres) =>
      assertEquals(unres, List.empty[InputMentions.Mention])
      assert(out.startsWith(reduced), s"转义还原失败: $out")
      assert(!out.contains("@@"), "交付文本不应再含双 @")
      assertEquals(out.drop(reduced.length), "\n\n[提及解析]\n[引用: 项目 · demo · /ws/demo]")
    }

  test("resolve：仅转义无提及时也还原（无指针块）"):
    val t = "联系 @@alice 处理"
    InputMentions.resolve(t, noneLookups).map { case (out, unres) =>
      assertEquals(out, "联系 @alice 处理")
      assertEquals(unres, List.empty[InputMentions.Mention])
    }

  test("resolve：// 转义仅作用于开头，还原为单斜杠（URL 不受影响）"):
    InputMentions.resolve("//compact 是什么", noneLookups).map { case (out, _) =>
      assertEquals(out, "/compact 是什么")
    } *> InputMentions.resolve("看 https://example.com/x", noneLookups).map { case (out, _) =>
      assertEquals(out, "看 https://example.com/x")
    }

  test("resolve：fail-open 全未解析则文本逐字节不变且回报清单"):
    val t = "看 @project:ghost 和 @src/missing/x"
    InputMentions.resolve(t, noneLookups).map { case (out, unres) =>
      assertEquals(out, t)
      assertEquals(unres.map(_.token), List("@project:ghost", "@src/missing/x"))
    }

  test("resolve：查询异常按未解析处理（fail-open）"):
    val boom = InputMentions.Lookups(
      file = _ => IO.raiseError(new RuntimeException("boom")),
      project = _ => IO.pure(None),
      flow = _ => IO.pure(None),
      session = _ => IO.pure(None),
      skill = _ => IO.pure(None)
    )
    val t = "看 @src/x.scala"
    InputMentions.resolve(t, boom).map { case (out, unres) =>
      assertEquals(out, t)
      assertEquals(unres.map(_.token), List("@src/x.scala"))
    }

  test("fileLookup：相对路径基于根解析，缺失返回 None"):
    val tmp = Files.createTempDirectory("mentions-spec")
    Files.writeString(tmp.resolve("note.md"), "hello")
    val lk = InputMentions.fileLookup(IO.pure(tmp.toString))
    lk("note.md").map { hit =>
      assertEquals(hit.map(p => p.replace('\\', '/').endsWith("/note.md")), Some(true))
    } *> lk("missing.md").map(r => assertEquals(r, None))

  test("fileLookup：以 / 开头的路径按根相对解析"):
    val tmp = Files.createTempDirectory("mentions-spec2")
    Files.createDirectories(tmp.resolve("src/main"))
    Files.writeString(tmp.resolve("src/main/A.scala"), "object A")
    val lk = InputMentions.fileLookup(IO.pure(tmp.toString))
    lk("/src/main/A.scala").map { hit =>
      assertEquals(hit.map(p => p.replace('\\', '/').endsWith("/src/main/A.scala")), Some(true))
    }
end InputMentionsSpec
