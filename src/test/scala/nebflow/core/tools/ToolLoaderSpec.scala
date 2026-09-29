package nebflow.core.tools

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import munit.CatsEffectSuite
import io.circe.Json
import io.circe.JsonObject
import io.circe.syntax.*
import nebflow.shared.PathUtil

class ToolLoaderSpec extends CatsEffectSuite:
  private val tempRoot: os.Path = os.pwd / "target" / "test-tool-loader"
  private val originalRoot = PathUtil.dataRoot
  PathUtil.setDataRoot(tempRoot)

  override def afterAll(): Unit =
    PathUtil.setDataRoot(originalRoot)

  private def toolsDir = tempRoot / "tools"

  /** Reset all layer dirs and registry state before each test. */
  private def resetState(): IO[Unit] =
    IO.delay {
      List("tools", "teams", "flows", "agents").foreach { sub =>
        val dir = tempRoot / sub
        if os.exists(dir) then os.remove.all(dir)
      }
      os.makeDir.all(toolsDir)
    } *> ToolLoader.reload().void

  private def writeToolConfig(
    dir: os.Path,
    file: String,
    toolName: String,
    description: String,
    command: String = "echo hello"
  ): Unit =
    val config = Json.obj(
      "name" -> Json.fromString(toolName),
      "description" -> Json.fromString(description),
      "command" -> Json.fromString(command),
      "inputSchema" -> Json.obj(
        "type" -> Json.fromString("object"),
        "properties" -> Json.obj()
      )
    )
    os.makeDir.all(dir)
    os.write(dir / s"$file.json", config.noSpaces)

  end writeToolConfig

  private def writeTool(name: String, toolName: String = null): Unit =
    val actualName = if toolName == null then name else toolName
    writeToolConfig(toolsDir, name, actualName, s"Test tool $actualName")

  test("reload registers new tool from disk"):
    for
      _ <- resetState()
      _ <- IO(writeTool("mytool"))
      _ <- ToolLoader.reload()
      toolMap = ToolRegistry.TOOL_MAP
    yield assert(toolMap.contains("mytool"), "Tool should be registered after reload")

  test("reload picks up modified tool name"):
    for
      _ <- resetState()
      // Initial load with name "tool-v1"
      _ <- IO(writeTool("mytool", "tool-v1"))
      _ <- ToolLoader.reload()
      map1 = ToolRegistry.TOOL_MAP
      _ = assert(map1.contains("tool-v1"), "tool-v1 should be registered")
      _ = assert(!map1.contains("tool-v2"), "tool-v2 should not exist yet")
      // Modify: change name inside JSON
      _ <- IO {
        os.remove(toolsDir / "mytool.json")
        writeTool("mytool", "tool-v2")
      }
      _ <- ToolLoader.reload()
      map2 = ToolRegistry.TOOL_MAP
    yield
      assert(!map2.contains("tool-v1"), "tool-v1 should be unregistered after name change")
      assert(map2.contains("tool-v2"), "tool-v2 should be registered after name change")

  test("reload unregisters tool when JSON file is deleted"):
    for
      _ <- resetState()
      _ <- IO {
        writeTool("tool-a")
        writeTool("tool-b")
      }
      _ <- ToolLoader.reload()
      map1 = ToolRegistry.TOOL_MAP
      _ = assert(map1.contains("tool-a"), "tool-a should be registered")
      _ = assert(map1.contains("tool-b"), "tool-b should be registered")
      // Delete one tool
      _ <- IO(os.remove(toolsDir / "tool-a.json"))
      _ <- ToolLoader.reload()
      map2 = ToolRegistry.TOOL_MAP
    yield
      assert(!map2.contains("tool-a"), "tool-a should be unregistered after file deletion")
      assert(map2.contains("tool-b"), "tool-b should still be registered")

  test("reload skips invalid JSON without crashing"):
    for
      _ <- resetState()
      _ <- IO {
        writeTool("good-tool")
        os.write(toolsDir / "bad-tool.json", "{ invalid json }")
      }
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield
      assert(map.contains("good-tool"), "Valid tool should be registered")
      assert(!map.contains("bad-tool"), "Invalid tool should be skipped")

  test("reload does not overwrite built-in tools"):
    for
      _ <- resetState()
      _ <- IO(writeTool("fake-builtin", "Bash")) // name conflicts with built-in
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
      bashTool = map.get("Bash")
    yield assert(bashTool.isInstanceOf[Some[?]], "Built-in Bash should not be overwritten")

  test("reload handles empty tools directory"):
    for
      _ <- resetState()
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield assert(map.contains("Read"), "Built-in tools should still be present")

  // --- Three-layer loading ---

  test("reload loads tools from team layer"):
    for
      _ <- resetState()
      _ <- IO(writeToolConfig(tempRoot / "teams" / "myteam" / "tools", "team-tool", "team-tool", "team-description"))
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield
      assert(map.contains("team-tool"), "Team-layer tool should be registered")
      assertEquals(map("team-tool").description, "team-description")

  test("reload loads tools from flow layer"):
    for
      _ <- resetState()
      _ <- IO(writeToolConfig(tempRoot / "flows" / "myflow" / "tools", "flow-tool", "flow-tool", "flow-description"))
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield
      assert(map.contains("flow-tool"), "Flow-layer tool should be registered")
      assertEquals(map("flow-tool").description, "flow-description")

  test("conflict priority: flow > team > global"):
    for
      _ <- resetState()
      _ <- IO {
        writeToolConfig(toolsDir, "dup", "dup", "global-desc")
        writeToolConfig(tempRoot / "teams" / "alpha" / "tools", "dup", "dup", "team-desc")
        writeToolConfig(tempRoot / "flows" / "beta" / "tools", "dup", "dup", "flow-desc")
      }
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield
      assert(map.contains("dup"))
      assertEquals(map("dup").description, "flow-desc", "Flow layer should override team and global")

  test("conflict priority: team > global"):
    for
      _ <- resetState()
      _ <- IO {
        writeToolConfig(toolsDir, "dup", "dup", "global-desc")
        writeToolConfig(tempRoot / "teams" / "alpha" / "tools", "dup", "dup", "team-desc")
      }
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield
      assert(map.contains("dup"))
      assertEquals(map("dup").description, "team-desc", "Team layer should override global")

  test("team-layer tool cannot override built-in"):
    for
      _ <- resetState()
      _ <- IO(writeToolConfig(tempRoot / "teams" / "alpha" / "tools", "fake", "Bash", "team-bash"))
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield
      assert(map.contains("Bash"))
      assert(!map("Bash").isInstanceOf[ScriptTool], "Built-in Bash must not be replaced by team tool")

  test("tools unregister cleanly across layers"):
    for
      _ <- resetState()
      _ <- IO {
        writeToolConfig(tempRoot / "teams" / "myteam" / "tools", "team-tool", "team-tool", "team-desc")
        writeToolConfig(tempRoot / "flows" / "myflow" / "tools", "flow-tool", "flow-tool", "flow-desc")
      }
      _ <- ToolLoader.reload()
      map1 = ToolRegistry.TOOL_MAP
      _ = assert(map1.contains("team-tool") && map1.contains("flow-tool"), "Both layer tools should load")
      // Remove team tools dir entirely
      _ <- IO(os.remove.all(tempRoot / "teams" / "myteam"))
      _ <- ToolLoader.reload()
      map2 = ToolRegistry.TOOL_MAP
    yield
      assert(!map2.contains("team-tool"), "Team tool should be unregistered after dir removal")
      assert(map2.contains("flow-tool"), "Flow tool should survive team dir removal")

  // --- $TOOL_DIR environment variable ---
  // 注（P1-4 批修复，环境确定性）：断言面 = ScriptTool 公开的 `toolDir` 字段——它是
  // TOOL_DIR 的唯一取值源（ScriptTool.call 里 pb.environment().put("TOOL_DIR",
  // toolDir.toString)），不再起 sh 子进程回读：①Windows/MSYS 下 loadFromDir 的文本
  // 替换会把 "$TOOL_DIR" 变成命令内联路径，在 sh -c 双引号解析里丢反斜杠（本机实测
  // 内联 "C:\a\b" ⇒ "C:ab"，env 腿完好）；②门禁/沙箱环境不保证 sh 可用。解析语义
  // （TOOL_DIR 指向持有该配置的目录、按层解析）由字段钉住，跨平台确定性。

  test("TOOL_DIR env var points at the tool config directory"):
    for
      _ <- resetState()
      _ <- IO(writeToolConfig(toolsDir, "echo-tool", "echo-tool", "echo"))
      _ <- ToolLoader.reload()
      tool = ToolRegistry.TOOL_MAP("echo-tool")
    yield tool match
      case st: ScriptTool => assertEquals(st.toolDir, toolsDir)
      case other => fail(s"expected a ScriptTool for 'echo-tool', got '${other.name}'")

  test("TOOL_DIR is set per-layer for team tools"):
    for
      _ <- resetState()
      teamTools = tempRoot / "teams" / "myteam" / "tools"
      _ <- IO(writeToolConfig(teamTools, "echo-team", "echo-team", "echo"))
      _ <- ToolLoader.reload()
      tool = ToolRegistry.TOOL_MAP("echo-team")
    yield tool match
      case st: ScriptTool => assertEquals(st.toolDir, teamTools)
      case other => fail(s"expected a ScriptTool for 'echo-team', got '${other.name}'")

  // --- Agent directory tools (three-layer agent dirs) ---
  // 2026-09-06 起 per-agent 层（agents/*/tools 与 teams/flows/*/agents/*/tools）
  // 整体退役——工具面收敛为 global/team/flow 三层；下述用例钉死「不再扫描」语义。

  test("per-agent tools/ subfolders are no longer scanned (retired 2026-09-06)"):
    for
      _ <- resetState()
      _ <- IO(writeToolConfig(tempRoot / "agents" / "myagent" / "tools", "agent-tool", "agent-tool", "agent-desc"))
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield assert(!map.contains("agent-tool"), "agent-dir tool must NOT be registered (per-agent layer retired)")

  test("nested team/flow agent tools/ subfolders are no longer scanned (retired 2026-09-06)"):
    for
      _ <- resetState()
      _ <- IO {
        writeToolConfig(
          tempRoot / "teams" / "myteam" / "agents" / "helper" / "tools",
          "team-agent-tool",
          "team-agent-tool",
          "team-agent-desc"
        )
        writeToolConfig(
          tempRoot / "flows" / "myflow" / "agents" / "worker" / "tools",
          "flow-agent-tool",
          "flow-agent-tool",
          "flow-agent-desc"
        )
      }
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield assert(
      !map.contains("team-agent-tool") && !map.contains("flow-agent-tool"),
      "nested agent-dir tools must NOT be registered (per-agent layer retired)"
    )

  test("conflict priority: team scope > agent dir (agent dir retired, team > global)"):
    for
      _ <- resetState()
      _ <- IO {
        writeToolConfig(toolsDir, "dup", "dup", "global-desc")
        writeToolConfig(tempRoot / "agents" / "myagent" / "tools", "dup", "dup", "agent-desc")
        writeToolConfig(tempRoot / "teams" / "alpha" / "tools", "dup", "dup", "team-desc")
      }
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield assertEquals(
      map("dup").description,
      "team-desc",
      "Team scope tool should override global (agent-dir layer retired)"
    )

  test("agent-dir $TOOL_DIR tools are no longer loaded (retired 2026-09-06)"):
    for
      _ <- resetState()
      agentTools = tempRoot / "agents" / "myagent" / "tools"
      _ <- IO(writeToolConfig(agentTools, "deploy", "deploy", "deploy", "node $TOOL_DIR/deploy.cjs"))
      loaded <- ToolLoader.loadAll()
    yield assert(loaded.forall(_._1.name != "deploy"), "agent-dir tool must NOT be loaded (per-agent layer retired)")

  test("global tools still work with $TOOL_DIR replacement"):
    for
      _ <- resetState()
      _ <- IO(writeToolConfig(toolsDir, "g-tool", "g-tool", "echo", "echo $TOOL_DIR"))
      loaded <- ToolLoader.loadAll()
      cfg = loaded.collectFirst { case (c, _) if c.name == "g-tool" => c }.get
    yield assertEquals(
      cfg.command,
      s"echo ${toolsDir.toString}",
      "Global tool $TOOL_DIR should resolve to the global tools dir"
    )

  // ============================================================
  // Subdirectory layout: tools/<name>/tool.json
  // ============================================================

  private def writeSubDirTool(
    parentDir: os.Path,
    subDirName: String,
    toolName: String,
    command: String = "echo hello"
  ): Unit =
    val subDir = parentDir / subDirName
    os.makeDir.all(subDir)
    val config = Json.obj(
      "name" -> Json.fromString(toolName),
      "description" -> Json.fromString(s"Subdir tool $toolName"),
      "command" -> Json.fromString(command),
      "inputSchema" -> Json.obj("type" -> Json.fromString("object"), "properties" -> Json.obj())
    )
    os.write(subDir / "tool.json", config.noSpaces)

  end writeSubDirTool

  test("subdirectory layout: tools/<name>/tool.json is loaded"):
    for
      _ <- resetState()
      _ <- IO(writeSubDirTool(toolsDir, "issue", "issue"))
      _ <- ToolLoader.reload()
      map = ToolRegistry.TOOL_MAP
    yield assert(map.contains("issue"), "subdirectory tool should be registered")

  test("subdirectory layout: $TOOL_DIR resolves to the subdirectory, not the parent"):
    for
      _ <- resetState()
      _ <- IO(writeSubDirTool(toolsDir, "issue", "issue", "bash $TOOL_DIR/issue.sh"))
      loaded <- ToolLoader.loadAll()
      cfg = loaded.collectFirst { case (c, _) if c.name == "issue" => c }.get
    yield assertEquals(
      cfg.command,
      // 期望值经 os.Path 拼接（P1-4 批修复，Windows 环境红）：子目录 sourceDir 的段分隔符
      // 由 os.Path 按平台渲染（Windows 为 '\'），旧写法字面量 "/issue" 在本机与替换结果
      // 差一个分隔符。语义不变：$TOOL_DIR 解析到**子目录**而非父目录。
      s"bash ${(toolsDir / "issue").toString}/issue.sh",
      "$TOOL_DIR should resolve to the subdirectory path"
    )

  test("flat and subdirectory layouts coexist"):
    for
      _ <- resetState()
      _ <- IO {
        writeToolConfig(toolsDir, "flat-tool", "flat-tool", "flat tool")
        writeSubDirTool(toolsDir, "sub-tool", "sub-tool", "sub tool")
      }
      loaded <- ToolLoader.loadAll()
      names = loaded.map(_._1.name).toSet
    yield
      assert(names.contains("flat-tool"), "flat layout tool loaded")
      assert(names.contains("sub-tool"), "subdirectory layout tool loaded")

  test("subdirectory layout: invalid tool.json is skipped, valid ones kept"):
    for
      _ <- resetState()
      _ <- IO {
        writeSubDirTool(toolsDir, "good", "good")
        // Write invalid JSON to a subdir
        val badDir = toolsDir / "bad"
        os.makeDir.all(badDir)
        os.write(badDir / "tool.json", "{ invalid json }")
      }
      loaded <- ToolLoader.loadAll()
      names = loaded.map(_._1.name).toSet
    yield
      assert(names.contains("good"), "valid subdir tool kept")
      assert(!names.contains("bad"), "invalid subdir tool skipped")

  test("agent-dir subdirectory layout is no longer scanned (retired 2026-09-06)"):
    for
      _ <- resetState()
      agentTools = tempRoot / "agents" / "myagent" / "tools"
      _ <- IO(writeSubDirTool(agentTools, "deploy", "deploy", "node $TOOL_DIR/deploy.cjs"))
      loaded <- ToolLoader.loadAll()
    yield assert(
      loaded.forall(_._1.name != "deploy"),
      "agent-dir subdir tool must NOT be loaded (per-agent layer retired)"
    )
end ToolLoaderSpec
