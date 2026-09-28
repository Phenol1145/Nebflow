package nebflow.ir

import cats.effect.IO
import io.circe.syntax.*
import io.circe.{Json, JsonObject}

/**
 * P0 参考命令（标准 §7.7，本节是**规范**）：`dev:fs:ls` 与 `dev:fs:cat`。
 *
 * 这两个命令是 C1/C2/C6/C7/C8/C14/C15/C23 的载体：实现它们即覆盖 P0 的管道、类型匹配
 * 与策略验收面。两条都要点：
 *  - `dev:fs:ls` 必须按 `name` **字典序稳定排序**（否则测试与 diff 不稳定）；
 *  - `dev:fs:cat` 有 `path` 读文件、无 `path` 读 stdin —— 这正是 [D19]「声明的是能力，
 *    不是必然行为」的由来，也是 `default` **不注入**的理由（注入后两者无法区分）。
 */
object DevCommands:

  /** 两命令共用的 argsSchema（§7.7 规范原文）。 */
  val fsSchema: JsonObject =
    Json
      .obj(
        "type" -> "object".asJson,
        "properties" -> Json.obj("path" -> Json.obj("type" -> "string".asJson, "default" -> ".".asJson)),
        "additionalProperties" -> false.asJson
      )
      .asObject
      .get

  val fsLs: CommandDef = CommandDef(
    name = "dev:fs:ls",
    description = "List VFS entries as JSONL: {name,path,kind,size?} sorted by name.",
    argsSchema = fsSchema,
    binding = Binding.Dev(lsHandler),
    io = CommandIo(stdin = None, stdout = StreamKind.Jsonl),
    pathArgs = Set("path"),
    caps = Set(Cap.FsRead(".")),
    delivery = Delivery.Both,
    trust = Trust.Builtin,
    audiences = Set(Audience.Human)
  )

  val fsCat: CommandDef = CommandDef(
    name = "dev:fs:cat",
    description = "Read a VFS file (path) or copy stdin (no path) as text.",
    argsSchema = fsSchema,
    binding = Binding.Dev(catHandler),
    io = CommandIo(stdin = Some(StreamKind.Text), stdout = StreamKind.Text),
    pathArgs = Set("path"),
    caps = Set(Cap.FsRead(".")),
    delivery = Delivery.Both,
    trust = Trust.Builtin,
    audiences = Set(Audience.Human)
  )

  /** 内置命令表（装配面 `CommandRegistry` 的种子；P1 起逐个把现有 `Tool` 注册为 `dev:*`）。 */
  def base: List[CommandDef] = List(fsLs, fsCat)

  // lazy：两个命令的 val 在 handler 之上初始化，非 lazy 会拿到 null（对象初始化顺序）
  private lazy val lsHandler: DevHandler = (_, ctx) =>
    IO.blocking {
      val canon = ctx.pathArgs.getOrElse("path", VfsPath.Canon(Nil))
      val dir = ctx.resolveArg(canon)
      if !os.exists(dir) then Left(notFound(dir))
      else if !os.isDir(dir) then Left(IrError.commandFailed(s"not a directory: ${dir.toString}"))
      else
        val entries = os.list(dir).toList.sortBy(_.last)
        Right(
          StreamValue.Jsonl(
            entries.map { p =>
              val child = canon.segments :+ p.last
              val isDir = os.isDir(p)
              Json
                .obj(
                  "name" -> p.last.asJson,
                  "path" -> child.mkString("/").asJson,
                  "kind" -> (if isDir then "dir" else "file").asJson
                )
                .deepMerge(if isDir then Json.obj() else Json.obj("size" -> os.size(p).asJson))
            }
          )
        )
      end if
    }.handleErrorWith(e => IO.pure(Left(IrError.commandFailed(e.getMessage))))

  private lazy val catHandler: DevHandler = (_, ctx) =>
    IO.blocking {
      ctx.pathArgs.get("path") match
        case None =>
          // 无 path ⇒ 读 stdin（声明是能力不是义务，[D19]）
          Right(StreamValue.Text(ctx.stdin.map(_.asText).getOrElse("")))
        case Some(canon) =>
          val file = ctx.resolveArg(canon)
          if !os.exists(file) || os.isDir(file) then Left(notFound(file))
          else
            Executor.decodeUtf8(os.read.bytes(file)) match
              case Left(_) =>
                Left(
                  IrError
                    .commandFailed(s"file is not valid UTF-8: ${file.toString}")
                    .withDetail("path", file.toString.asJson)
                    .withDetail("reason", "binary_not_supported".asJson)
                )
              case Right(text) => Right(StreamValue.Text(text))
    }.handleErrorWith(e => IO.pure(Left(IrError.commandFailed(e.getMessage))))

  private def notFound(path: os.Path): IrError =
    IrError.commandFailed(s"no such file: ${path.toString}").withDetail("path", path.toString.asJson)

end DevCommands
