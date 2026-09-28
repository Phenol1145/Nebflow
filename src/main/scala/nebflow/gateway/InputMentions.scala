// 严格DAG收官后的首个 feature(feat/mention-tokens,2026-09-27):输入框 CLI 化——
// 自然语言中嵌入结构化 token(@实体/@技能),后端权威解析(Web/CLI 同构),仅指针注入。
// 设计红线:不改 AgentCommand/AgentState 模型(指针行追加进既有字符串字段,队列/重放/落盘
// 天然兼容);未解析 token 原样保留(fail-open);与斜杠命令封存闸(input.js 2026-08-29 裁定)无交集。
// 语法统一(2026-09-27 作者指令):只保留 / 与 @ 两个前缀——/ 执行、@ 引用;$ 退役,
// 技能引用改 @skill:(执行侧 /skill: 由前端派发,不在本文件);@@ 转义(双 @ 不解析,交付文本还原单 @)。
package nebflow.gateway

import cats.effect.IO
import nebflow.core.SessionStore
import nebflow.core.entity.EntityLoader
import nebflow.core.project.ProjectStore
import nebflow.core.skill.SkillService

/**
 * 提及 token 分词与解析。
 *
 * 语法(v2,2026-09-27 作者指令统一):
 *  - `@/abs` `@./rel` `@rel/path` `@~/x` —— 文件(相对会话 explorer 根;`@/x` 也按根相对,
 *    Windows 盘符路径原样);存在性校验,指针只带路径不带正文。
 *  - `@project:名称` / `@flow:名称` / `@session:名称或id` / `@skill:技能名` —— 带类型前缀的数据实体
 *    (技能名含 `ns/name` 命名空间形态;`$技能名` 已退役,$ 不再触发任何解析)。
 *  - `@@` 转义:双 @ 起首的 token 不解析为提及,交付文本还原为单 @(只吃一个 @,余下原样)。
 *
 * 分词边界:token 前一字符不是 ASCII 词字符(挡住邮箱 `user@x.com`,放行 CJK 紧邻
 * `看@project:x 的`);尾随标点剥离;`\@` 反斜杠转义不解析(文本保留,兼容旧习)。裸 `@词`
 * (无路径形态、无类型前缀)不是提及——自然语言零破坏。
 */
object InputMentions:

  sealed trait Mention:
    /** 原样 token(含前缀,已剥尾随标点)。 */
    def token: String

  final case class FileMention(token: String, path: String) extends Mention
  final case class ProjectMention(token: String, name: String) extends Mention
  final case class FlowMention(token: String, name: String) extends Mention
  final case class SessionMention(token: String, key: String) extends Mention
  final case class SkillMention(token: String, name: String) extends Mention

  /** 各实体类别的查询面:None = 无法解析(fail-open 原文保留)。返回值即指针所需最小数据。 */
  final case class Lookups(
    file: String => IO[Option[String]], // 原始路径 -> 存在的绝对路径
    project: String => IO[Option[(String, String)]], // 名称 -> (名称, workspace)
    flow: String => IO[Option[String]], // 名称 -> 确认存在的规范名
    session: String => IO[Option[(String, String)]], // 名称或id -> (名称, id)
    skill: String => IO[Option[(String, String)]] // 名称 -> (名称, 描述)
  )

  private val TokenRe = "@[^\\s]+".r

  // 尾随标点剥离(留在原文):中文常用 + ASCII 配对符 + markdown 强调尾星号。
  private val TrailingPunct: Set[Char] = "。，、；：！？）】」》…”\"'.,;:!?)]}>*".toSet

  // 句中终结符:中文无空格,token 必须在此截断,否则 `@project:demo。然后` 会把整句
  // 吞进 token(CJK 文件名/技能名不含这些句读符,截断安全)。
  private val TokenTerminators: Set[Char] = "。，、；：！？）】」》…”—".toSet

  private def isAsciiWordChar(c: Char): Boolean =
    (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'

  /**
   * 单趟扫描:产出(还原 `@@`/`//` 转义后的文本, 提及列表)。提及之间的原文一律不动;
   * `@@` 起首的 token 只吃一个 @(转义语义),不解析、不产生指针。
   */
  private def scan(text: String): (String, List[Mention]) =
    if text == null || text.isEmpty then (text, Nil)
    else
      // `//` 转义(2026-09-27 作者指令):只作用于输入**开头**(斜杠命令派发位),
      // 双斜杠还原为单斜杠——URL 等中段 `//` 不受影响;后端不解释斜杠命令,仅还原。
      val src = if text.startsWith("//") then text.drop(1) else text
      val out = StringBuilder()
      var last = 0
      val buf = List.newBuilder[Mention]
      for m <- TokenRe.findAllMatchIn(src) do
        val start = m.start
        val boundaryOk = start == 0 || !isAsciiWordChar(src.charAt(start - 1))
        val escaped = start > 0 && src.charAt(start - 1) == '\\'
        if boundaryOk && !escaped then
          var tok = m.matched
          val cut = tok.indexWhere(TokenTerminators.contains, 1)
          if cut > 0 then tok = tok.substring(0, cut)
          while tok.length > 1 && TrailingPunct.contains(tok.last) do tok = tok.init
          if tok.startsWith("@@") then
            // 转义(2026-09-27 作者指令):双 @ 不解析,交付文本还原为单 @。
            out.append(src.substring(last, start)).append(tok.drop(1))
            last = start + tok.length
          else classify(tok).foreach(buf += _)
      out.append(src.substring(last))
      (out.toString, buf.result())

  /** 纯分词:不查注册表。 */
  def tokenize(text: String): List[Mention] = scan(text)._2

  private def classify(token: String): Option[Mention] =
    if token.length < 2 then None
    else if token.startsWith("@project:") && token.length > 9 then Some(ProjectMention(token, token.drop(9)))
    else if token.startsWith("@flow:") && token.length > 6 then Some(FlowMention(token, token.drop(6)))
    else if token.startsWith("@session:") && token.length > 9 then Some(SessionMention(token, token.drop(9)))
    else if token.startsWith("@skill:") && token.length > 7 then Some(SkillMention(token, token.drop(7)))
    else if token.startsWith("@") && (token.contains("/") || token.startsWith("@~")) then
      Some(FileMention(token, token.drop(1)))
    else None // 裸 @词 不是提及(邮箱/普通符号场景零破坏)

  /**
   * 解析并生成指针块。返回 (增强后文本, 未解析提及)。
   * 有可解析提及 ⇒ 在**还原 `@@` 转义后的文本**尾部追加 `\n\n[提及解析]\n` + 每提及一行的
   * 指针(去重保序);既无提及也无转义 ⇒ 文本逐字节不变。查询异常按 None 处理(fail-open)。
   */
  def resolve(text: String, lookups: Lookups): IO[(String, List[Mention])] =
    val (reduced, mentions) = scan(text)
    if mentions.isEmpty then IO.pure((reduced, Nil))
    else
      mentions
        .foldLeft(IO.pure((List.empty[String], List.empty[Mention]))) { (acc, m) =>
          acc.flatMap { case (lines, unresolved) =>
            pointerFor(m, lookups).map {
              case Some(line) => (lines :+ line, unresolved)
              case None => (lines, unresolved :+ m)
            }
          }
        }
        .map { case (lines, unresolved) =>
          val out =
            if lines.isEmpty then reduced
            else reduced + "\n\n[提及解析]\n" + lines.distinct.mkString("\n")
          (out, unresolved)
        }

    end if

  end resolve

  private def pointerFor(m: Mention, l: Lookups): IO[Option[String]] =
    val q: IO[Option[String]] = m match
      case FileMention(_, p) =>
        l.file(p).map(_.map(abs => s"[引用: 文件 · ${baseName(abs)} · $abs]"))
      case ProjectMention(_, n) =>
        l.project(n).map(_.map((pn, ws) => s"[引用: 项目 · $pn · $ws]"))
      case FlowMention(_, n) =>
        l.flow(n).map(_.map(fn => s"[引用: 流程 · $fn]"))
      case SessionMention(_, k) =>
        l.session(k).map(_.map((sn, id) => s"[引用: 会话 · $sn · $id]"))
      case SkillMention(_, n) =>
        l.skill(n).map(_.map((sn, d) => if d.nonEmpty then s"[技能: $sn — $d]" else s"[技能: $sn]"))
    q.handleErrorWith(_ => IO.pure(None))

  private def baseName(p: String): String =
    val i = p.lastIndexOf('/')
    val j = p.lastIndexOf('\\')
    val cut = math.max(i, j)
    if cut >= 0 && cut < p.length - 1 then p.substring(cut + 1) else p

  // ── 生产装配(从网关既有能力拼装默认查询面) ──────────────────────────

  /**
   * 文件查询:相对/`~`/盘符绝对三种形态 → 存在性校验后的绝对路径。
   * `root` 为按需求值的 IO(explorer 根)——消息不含文件提及则零开销。
   */
  def fileLookup(root: IO[String]): String => IO[Option[String]] = raw =>
    root.flatMap { r =>
      IO {
        val p = expandPath(raw, r)
        val f = java.nio.file.Paths.get(p).normalize.toFile
        if f.exists then Some(f.getAbsolutePath.replace('\\', '/')) else None
      }.handleErrorWith(_ => IO.pure(None))
    }

  private def expandPath(raw: String, root: String): String =
    val joined =
      if raw == "~" || raw.startsWith("~/") then
        java.nio.file.Paths.get(System.getProperty("user.home"), raw.drop(1)).toString
      else if isDriveAbsolute(raw) then raw
      else if raw.startsWith("/") then root + raw // "/x" 在 Windows 语义歧义,统一按根相对
      else if raw.startsWith("./") then root + raw.drop(1)
      else java.nio.file.Paths.get(root, raw).toString
    joined.replace('\\', '/')

  private def isDriveAbsolute(s: String): Boolean =
    s.length >= 3 && s.charAt(1) == ':' && (s.charAt(2) == '/' || s.charAt(2) == '\\')

  /** 生产默认查询面(explorer 根按需求值;`@skill:` 名精确匹配,命名空间形态含 `/`)。 */
  def defaultLookups(sessionId: String, explorerRoot: IO[String], store: SessionStore): Lookups =
    Lookups(
      file = fileLookup(explorerRoot),
      project = name => ProjectStore.list().map(_.find(_.name.equalsIgnoreCase(name)).map(p => (p.name, p.workspace))),
      flow = name => EntityLoader.listFlows().map(m => m.keys.find(_.equalsIgnoreCase(name))),
      session = key =>
        store.listSessions.map(
          _.find(m => m.id == key || m.name.equalsIgnoreCase(key)).map(m => (m.name, m.id))
        ),
      skill = name => SkillService.listSkills().map(_.find(_.name == name).map(s => (s.name, s.description)))
    )

end InputMentions
