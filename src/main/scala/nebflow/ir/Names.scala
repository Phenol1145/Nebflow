package nebflow.ir

import io.circe.{Json, JsonObject}

import scala.concurrent.duration.*

/**
 * 命令名语法与规范化（标准 §7.3 N1–N5）。
 *
 * ```
 * name    := segment (":" segment)+
 * segment := [a-z0-9] [a-z0-9_-]*        （长度 ≤ 32）
 * name 总长 ≤ 96
 * ```
 *
 * N3：外部标识（MCP serverId/tool、扩展名）**可以**含大写或非法字符，此时**必须**
 * 做**规范化**产生 IR 名，且**原始标识必须保存在 `binding` 内**——IR 名到实现的
 * 双射由 `binding` 保证，**禁止**靠字符串反解。N4：规范化必须是确定性函数且必须
 * 在注册期检测碰撞。
 */
object Names:

  val SegmentMax = 32
  val NameMax = 96

  private val SegmentRe = "^[a-z0-9][a-z0-9_-]*$".r

  /** 段集合（R3/N1/N2 的语法面，不含「首段必须与 binding 一致」——那条在注册期查）。 */
  def syntax(name: String): Either[IrError, Unit] =
    if name.isEmpty then Left(IrError.nameInvalid(name, "empty"))
    else if name.length > NameMax then Left(IrError.nameInvalid(name, s"longer than $NameMax"))
    else
      val segments = name.split(":", -1).toList
      if segments.length < 2 then Left(IrError.nameInvalid(name, "needs at least two ':'-separated segments"))
      else
        segments.zipWithIndex.collectFirst {
          case (seg, i) if seg.length > SegmentMax => s"segment #$i longer than $SegmentMax"
          case (seg, i) if SegmentRe.findFirstIn(seg).isEmpty => s"segment #$i is not [a-z0-9][a-z0-9_-]*"
          case ("", i) => s"segment #$i is empty"
        } match
          case Some(reason) => Left(IrError.nameInvalid(name, reason))
          case None => Right(())

  /**
   * 确定性规范化（N3/N4）：小写 + 非法字符 → `_`。**仅用于外部面**（`mcp:`/`ext:`）
   * 的标识；`dev:`/`bash:` 是自撰名字，非法即拒（不静默改写自撰名）。
   */
  def normalize(name: String): String =
    name.toLowerCase
      .split(":", -1)
      .map { seg =>
        val mapped = seg.map { c =>
          if (c.isLetterOrDigit && c.toInt < 128) || c == '_' || c == '-' then c else '_'
        }
        // 首字符必须 [a-z0-9]（'-'/'_' 起头 → 前置 'x'，仍确定性）
        if mapped.isEmpty then "x"
        else if mapped.head == '_' || mapped.head == '-' then "x" + mapped
        else mapped
      }
      .mkString(":")

end Names
