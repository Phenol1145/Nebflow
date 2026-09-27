package nebflow.core

import cats.effect.IO
import cats.syntax.all.*
import nebflow.actor.RootAgentIdentity
import nebflow.shared.*

import java.nio.file.{Files, Paths}

object Repl:
  private val IMAGE_EXTENSIONS = List(".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg")
  private val VIDEO_EXTENSIONS = List(".mp4", ".mov", ".avi", ".mkv", ".webm", ".flv", ".wmv", ".m4v")
  private val MEDIA_EXTENSIONS = IMAGE_EXTENSIONS ++ VIDEO_EXTENSIONS

  private val MEDIA_REGEX =
    s"(?:^|\\s)((?:/[^\\s]+|[~.][^\\s]+)\\.(?:${MEDIA_EXTENSIONS.map(_.drop(1)).mkString("|")}))".r

  private val MIME_MAP = Map(
    "png" -> "image/png",
    "jpg" -> "image/jpeg",
    "jpeg" -> "image/jpeg",
    "gif" -> "image/gif",
    "webp" -> "image/webp",
    "bmp" -> "image/bmp",
    "svg" -> "image/svg+xml",
    "mp4" -> "video/mp4",
    "mov" -> "video/quicktime",
    "avi" -> "video/x-msvideo",
    "mkv" -> "video/x-matroska",
    "webm" -> "video/webm",
    "flv" -> "video/x-flv",
    "wmv" -> "video/x-ms-wmv",
    "m4v" -> "video/x-m4v"
  )

  private val MAX_IMAGE_SIZE = 5 * 1024 * 1024 // 5MB

  // 2026-09-28 裁定（ORCH5-P3 / ORCH5-R1：死码清册 C3 条）——本处原 `replaceMediaPaths`
  // 与 `buildUserMessage` 两成员已**整删**，零引用实证（全仓 `git grep -n` 含测试树/
  // 字符串/反射名）：`buildUserMessage` 仅命中本文件定义行；`replaceMediaPaths` 唯一
  // 调用点 = `buildUserMessage` 体内（两成员同删，调用面随之消失）。删除后 `Repl`
  // 仅剩活成员 `loadSystemPrompt`（消费点 = `agent/AgentSessionExecution.scala:2235`），
  // 非空壳；两成员上方的 `// public for AgentActor` 行内注释随成员同删（非带日期
  // 注释 ⇒ 无迁置义务）。私有 vals（`IMAGE_EXTENSIONS` … `MAX_IMAGE_SIZE`）按同裁定
  // 「文件其余逐字不动」原样留置（不引 -Wunused，零新警告）。

  def loadSystemPrompt(): String = // public for AgentActor
    val path = PathUtil.dataRoot / "agents" / RootAgentIdentity.Name / "system.md"
    if os.exists(path) then os.read(path) else ""
end Repl
