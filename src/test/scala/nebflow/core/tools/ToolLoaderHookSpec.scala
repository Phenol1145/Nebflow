package nebflow.core.tools

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite
import io.circe.Json
import nebflow.shared.PathUtil

/**
 * P1-4（ext: 桥挂钩）一致性用例：[[ToolLoader.reload]] 的 post-registration 钩——
 * boot 与 watcher 共用 reload() 单入口，钩收到**过滤后**的声明级 (config, sourceDir) 对；
 * 未设钩不炸；钩抛异常被吞（handleErrorWith ERROR）且 reload 照常完成（watcher 线程不死）。
 *
 * dataRoot 隔离用 beforeEach/afterEach 存取（GlobalSafetySpec 配方，无构造顺序风险）；
 * 钩在每用例起点归零 + afterAll 兜底（ToolLoader 是进程级单例，防跨套件泄漏）。
 */
class ToolLoaderHookSpec extends CatsEffectSuite:

  private val tempRoot: os.Path = os.pwd / "target" / "test-tool-loader-hook"
  private var savedRoot: os.Path = null

  override def beforeEach(context: BeforeEach): Unit =
    savedRoot = PathUtil.dataRoot
    PathUtil.setDataRoot(tempRoot)
    ToolLoader.setReloadHook(_ => IO.unit) // 每用例起点归零

  override def afterEach(context: AfterEach): Unit =
    ToolLoader.setReloadHook(_ => IO.unit)
    PathUtil.setDataRoot(savedRoot)

  private def toolsDir = tempRoot / "tools"

  private def resetState(): IO[Unit] =
    IO.delay {
      List("tools", "teams", "flows").foreach { sub =>
        val dir = tempRoot / sub
        if os.exists(dir) then os.remove.all(dir)
      }
      os.makeDir.all(toolsDir)
    } *> ToolLoader.reload().void

  private def writeToolConfig(dir: os.Path, file: String, toolName: String): Unit =
    val config = Json.obj(
      "name" -> Json.fromString(toolName),
      "description" -> Json.fromString(s"hook test tool $toolName"),
      "command" -> Json.fromString("echo hello"),
      "inputSchema" -> Json.obj(
        "type" -> Json.fromString("object"),
        "properties" -> Json.obj()
      )
    )
    os.makeDir.all(dir)
    os.write(dir / s"$file.json", config.noSpaces)

  test("reload 触发钩且收到过滤后 (config,dir) 对；与内置名冲突的件不进钩、内置件不被覆盖"):
    for
      seen <- Ref.of[IO, List[List[(String, String, os.Path)]]](Nil) // (name, layer, sourceDir)
      builtinRead = ToolRegistry.TOOL_MAP.getOrElse("Read", fail("builtin Read missing"))
      _ <- IO(
        ToolLoader.setReloadHook(pairs => seen.update(_ :+ pairs.map((c, d) => (c.name, c.layer, d)).sortBy(_._1)))
      )
      _ <- resetState()
      _ <- IO(writeToolConfig(toolsDir, "hooktool", "hooktool"))
      _ <- IO(writeToolConfig(toolsDir, "conflict-read", "Read")) // 与内置同名 ⇒ 同谓词过滤
      _ <- ToolLoader.reload()
      snaps <- seen.get
    yield
      // resetState 的 reload 也过钩（空表）；最后一批 = 过滤后的声明级对（flat 布局 ⇒ dir=toolsDir）
      assertEquals(snaps.last, List(("hooktool", "global", toolsDir)))
      assertEquals(snaps.flatten.exists(_._1 == "Read"), false) // 冲突件不进钩
      assertEquals(ToolRegistry.TOOL_MAP.contains("hooktool"), true) // ScriptTool 照常注册
      assertEquals(ToolRegistry.isExternalTool("hooktool"), true)
      assertEquals(ToolRegistry.TOOL_MAP.get("Read").contains(builtinRead), true) // 内置件未被覆盖（同谓词的另一面）

  test("未设钩（缺省 IO.unit）reload 不炸"):
    for
      _ <- resetState()
      _ <- IO(writeToolConfig(toolsDir, "nohook", "nohook"))
      _ <- ToolLoader.reload()
    yield assertEquals(ToolRegistry.TOOL_MAP.contains("nohook"), true)

  test("钩抛异常被 handleErrorWith 吞且 reload 完成（TOOL_MAP 仍就位）"):
    for
      _ <- resetState()
      _ <- IO(ToolLoader.setReloadHook(_ => IO.raiseError(new RuntimeException("hook boom"))))
      _ <- IO(writeToolConfig(toolsDir, "boomtool", "boomtool"))
      _ <- ToolLoader.reload() // 若钩异常穿透，此处 IO 失败 ⇒ 用例红
    yield assertEquals(ToolRegistry.TOOL_MAP.contains("boomtool"), true)

end ToolLoaderHookSpec
