package nebflow.ir

import cats.effect.IO
import io.circe.syntax.*

/**
 * VFS 路径值域与守卫（标准 §4.6 P1–P9）。
 *
 * **禁止**把 IR 路径交给宿主机路径解析器（[P9]/[D28]）：`core/tools/ToolPathUtil.scala`
 * 对绝对路径**原样返回**（落到宿主根）且把 `~` 展开为**宿主** home —— 那是「最自然的
 * 实现路径」上的宿主逃逸点（`FsRead("/")` 想表达「整个 VFS」，沿用该 helper 就变成
 * 整盘读）。本对象的 canonical 化是**纯词法、面向 VFS** 的实现：`/` → VFS 根，
 * 以 VFS 根为基准，canonical 后按**段**比较（P2/P6）。
 *
 * 两级判定（[P7]/[D18]）：路径越出 VFS 根 ⇒ **计划非法**（exit 2 + `router.schema.bad_value`
 * —— VFS 根之外不存在）；路径在 VFS 内但越出命令声明的 cap 前缀 ⇒ **策略拒绝**
 * （exit 125 + `policy.denied`）。判据都是 canonical 化后的段序列。
 */
final case class VfsRoot(host: os.Path)

object VfsRoot:
  /** v1 的 VFS 根 = **会话可见根**（§4.6 尾）：由装配面单点解析后注入（[D20]）。 */
  def apply(host: String): VfsRoot = VfsRoot(os.Path(host, os.pwd))
end VfsRoot

object VfsPath:

  /** canonical 形态：相对 VFS 根的**段**序列（空 = VFS 根本身）。 */
  final case class Canon(segments: List[String]):
    def isRoot: Boolean = segments.isEmpty

    /** 展示形态：根 = `.`，其余为 `a/b/c`。 */
    def render: String = if segments.isEmpty then "." else segments.mkString("/")
  end Canon

  /**
   * 规范化（唯一实现，路径解析与 cap 判定共用 —— P6）。
   * 拒绝：宿主形态（Windows 盘符、UNC，P4）、`~`（P3）、`..` 越根（P2）。
   * Windows 分隔符 `\` 取**规范化**路线（P5：规范化并在 canonical 化后判定）。
   */
  def canon(raw: String): Either[IrError, Canon] =
    val normalized = raw.replace('\\', '/')
    if normalized.isEmpty then Left(IrError.badValue("path must not be empty"))
    else if normalized.startsWith("//") then
      Left(IrError.badValue(s"UNC host path is not part of the VFS: '$raw' ([P4])"))
    else if normalized.length >= 2 && normalized.charAt(1) == ':' && normalized.charAt(0).isLetter then
      Left(IrError.badValue(s"host drive path is not part of the VFS: '$raw' ([P4])"))
    else
      val segments = normalized.split("/", -1).toList.filter(s => s.nonEmpty && s != ".")
      segments.find(s => s == "~" || s.startsWith("~")) match
        case Some(bad) => Left(IrError.badValue(s"'~' is never expanded at the IR layer: '$bad' ([P3])"))
        case None =>
          segments
            .foldLeft[Either[IrError, List[String]]](Right(Nil)) { (acc, seg) =>
              acc.flatMap { rev =>
                if seg == ".." then
                  if rev.isEmpty then
                    Left(
                      IrError.badValue(
                        s"path escapes the VFS root: '$raw' ([P2]/[P7] — plan invalid, not a policy denial)"
                      )
                    )
                  else Right(rev.tail)
                else Right(seg :: rev)
              }
            }
            .map(rev => Canon(rev.reverse))
      end match
    end if
  end canon

  /**
   * cap 前缀（§8.1）的 canonical 化：与路径值同规则，但**允许**以 `.` 表示 VFS 根
   * （空前缀 = 全 VFS）。越根的 cap 声明是**注册/装配**错误，交回调用方处理。
   */
  def canonPrefix(raw: String): Either[IrError, Canon] =
    if raw == "." || raw == "/" then Right(Canon(Nil)) else canon(raw)

  /** 段级包含判定（P2 先例：`SandboxPolicy.contains` 的逐段 `startsWith`，天然带边界）。 */
  def isWithin(prefix: Canon, target: Canon): Boolean =
    target.segments.startsWith(prefix.segments)

  /** VFS canonical 路径 → 宿主路径。segments 无 `..`、无绝对段 ⇒ 拼接不逃逸。 */
  def resolve(root: VfsRoot, canon: Canon): os.Path =
    canon.segments.foldLeft(root.host)((acc, seg) => acc / seg)

  /**
   * **软链接感知**的 canonical 化（IO 面）：先走词法 [[canon]]，再把**已存在的部分**
   * realpath（内核语义 —— 正确解析符号链接与 `..`，绝不自行折叠；先例
   * `core/sandbox/SandboxPolicy.scala:343 canonicalize`），最后把结果折回 VFS 相对段。
   *
   * 为什么必须有这一步：段级判定若只看词法路径，根内一个指向根外的符号链接
   * （`vfs/link -> /etc/passwd`）就能把 `dev:fs:cat link` 变成宿主读 —— 与 [D28] 同族的
   * 「最自然的实现路径上的逃逸点」。越根 ⇒ `router.schema.bad_value`（[P7] 第一级：
   * VFS 根之外不存在），仍然在**执行前**（[P8]）由 [[Planner]] 校验步完成。
   *
   * 目标尚不存在（新文件）时 realpath 只到最深存在祖先，剩余段原样接回 —— 与
   * `SandboxPolicy.canonicalize` 同口径。
   */
  def canonResolved(root: VfsRoot, raw: String): IO[Either[IrError, Canon]] =
    canon(raw) match
      case Left(err) => IO.pure(Left(err))
      case Right(lexical) =>
        IO.blocking {
          val realRoot = realPath(root.host)
          val realTarget = realPath(resolve(root, lexical))
          if realTarget == realRoot then Right(Canon(Nil))
          else if !realTarget.startsWith(realRoot) then
            Left(
              IrError
                .badValue(s"path resolves outside the VFS root (symlink or reparse point): '$raw' ([P2]/[P7])")
                .withDetail("path", raw.asJson)
            )
          else
            // RelPath.toString 归一为 `/` 分隔（Windows 也是）⇒ 直接用文本分段，不赌 API 名
            val rel = realTarget.relativeTo(realRoot).toString
            Right(Canon(rel.split('/').toList.filter(s => s.nonEmpty && s != ".")))
        }.handleErrorWith(e => IO.pure(Left(IrError.badValue(s"path resolution failed for '$raw': ${e.getMessage}"))))

  /** `toRealPath` 等价：向上找最深存在祖先 realpath，再把剩余段接回（含 `.`/`..` 折叠）。 */
  private def realPath(p: os.Path): os.Path =
    var base = p.toNIO.toAbsolutePath
    var suffix: List[String] = Nil
    while base.getParent != null && !java.nio.file.Files.exists(base, java.nio.file.LinkOption.NOFOLLOW_LINKS) do
      suffix = base.getFileName.toString :: suffix
      base = base.getParent
    val real =
      try base.toRealPath()
      catch case _: Exception => base
    os.Path(
      suffix.foldLeft(real) { (acc, seg) =>
        seg match
          case ".." => Option(acc.getParent).getOrElse(acc)
          case "." | "" => acc
          case other => acc.resolve(other)
      }
    )
  end realPath

end VfsPath
