package nebflow.core.plugin

import cats.effect.IO
import io.circe.Json
import io.circe.syntax.*
import munit.CatsEffectSuite
import nebflow.shared.PathUtil

import scala.concurrent.duration.*

/**
 * 种子桥批（2026-09-30）——官方种子样例包 × 装载层校验 spec。
 *
 * 样例包（src/main/resources/seed/plugins/，只进 seed 树不进 manifest items）：
 *  - `mcp-echo-toolkit`：mcp.json 声明样例（stdio entry + ${PLUGIN_ROOT} args 占位
 *    符 + 零第三方依赖 python3 server）；
 *  - `curl-toolkit`：org.nebflow/tools.json 工具授予声明样例（恰为闭合形态
 *    {"tools":["Curl"]}）。
 *
 * 覆盖腿：
 * ① 装载成功——classpath 字节复制两包到隔离 plugins/ 根（OfficialIdentitySpec
 *    字节复制先例）→ scan/resolve Right → 组件面（echo stdio entry 原样、
 *    toolsExtension == List("Curl")）→ renderCatalog 行含 "mcp: echo" / "tools: Curl"；
 * ② 拒答-坏 schema——plugin.json 非 canonical $schema → 拒载；mcp.json 坏 $schema →
 *    MCP 组件 invalid 但包继续且 skills 照载（§6.2 边界）；tools.json
 *    {"tools":"Curl"} → 拒载（本批新规则：闭合 schema fail-closed）；
 * ③ 拒答-未知键——tools.json 加 "extra" 键 → 拒载（PLUGIN_TOOLS_SCHEMA）；
 *    mcp.json entry 未知键 → 告警+忽略+entry 照载（官方协议 report-and-ignore 臂，
 *    与 nebflow 自有命名空间的 fail-closed 形成对照）；
 * ④ 拒答-前缀违规——种子副本改名 nebflow-echo-imp → OFFICIAL_IMPERSONATION 拒载；
 * ⑤ 口径一致——validate_plugin.py 的 RESERVED_PREFIX 字面量 == 装载层
 *    OfficialPackages.ReservedPrefix；plugin-packaging SKILL.md 规则句无
 *    "nebflow-plugin-" 旧口径（字符串级断言，不 exec python，跨平台）。
 */
class PluginSeedBridgeSpec extends CatsEffectSuite:

  override val munitIOTimeout: FiniteDuration = 60.seconds

  private val tempRoot: os.Path = os.pwd / "target" / "test-plugin-seed-bridge"
  private val originalRoot = PathUtil.dataRoot

  PathUtil.setDataRoot(tempRoot)
  os.remove.all(tempRoot)
  os.makeDir.all(tempRoot / "plugins")
  os.write.over(tempRoot / "nebflow.json", "{}")

  override def afterAll(): Unit =
    PathUtil.setDataRoot(originalRoot)

  // ── classpath 资源面（seed 树读取；OfficialIdentitySpec 同款手法）────────

  private def resourceUrl(rel: String): java.net.URL =
    Option(getClass.getClassLoader.getResource(rel)).getOrElse(
      fail(s"classpath resource '$rel' not found — seed tree missing from test classpath")
    )

  /** seed 树内某包的目录（plugin.json 锚点定位，file:/jar: 由 loader 决定）。 */
  private def seedDir(name: String): os.Path =
    os.Path(java.nio.file.Paths.get(resourceUrl(s"seed/plugins/$name/plugin.json").toURI)) / os.up

  private def resourceText(rel: String): String =
    val in = resourceUrl(rel).openStream()
    try new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    finally in.close()

  /** 每测试隔离：清空 plugins/ + 复位 nebflow.json + 清 mtime 缓存。 */
  private def freshPlugins(): Unit =
    os.remove.all(tempRoot / "plugins")
    os.makeDir.all(tempRoot / "plugins")
    os.write.over(tempRoot / "nebflow.json", "{}")
    PluginRegistry.invalidateCache()

  /** classpath 字节复制种子包到隔离 plugins/ 根（destName 允许改名形态）。 */
  private def installSeed(name: String, destName: String = ""): os.Path =
    val dest = tempRoot / "plugins" / (if destName.isEmpty then name else destName)
    os.copy(seedDir(name), dest, createFolders = true, mergeFolders = true, replaceExisting = true)
    PluginRegistry.invalidateCache()
    dest

  private def rejectedOf(name: String): IO[Option[(String, String)]] =
    PluginRegistry.listWithRejected().map(_._2.find(_._1 == name))

  // ── ① 装载成功 ────────────────────────────────────────────

  test("① 装载成功: 种子字节副本 scan/resolve Right，echo stdio entry + toolsExtension=Curl + catalog 行") {
    freshPlugins()
    installSeed("mcp-echo-toolkit")
    installSeed("curl-toolkit")
    for
      loaded <- PluginRegistry.scan()
      mcpResolved <- PluginRegistry.resolve("mcp-echo-toolkit")
      curlResolved <- PluginRegistry.resolve("curl-toolkit")
      catalog <- PluginRegistry.renderCatalog()
    yield
      assertEquals(loaded.map(_.name).toSet, Set("mcp-echo-toolkit", "curl-toolkit"), s"both seeds must load, got $loaded")
      mcpResolved match
        case Left(err) => fail(s"mcp-echo-toolkit must resolve: $err")
        case Right(p) =>
          assertEquals(p.mcpServers.keySet, Set("echo"))
          val echo = p.mcpServers("echo")
          assertEquals(echo.command, Some("python3"), "bare command name → PATH search (§7.2.2)")
          assertEquals(
            echo.args,
            Some(List("${PLUGIN_ROOT}/scripts/echo-server.py")),
            "args keeps the placeholder verbatim — expanded at acquire time (PluginMcpManager.runtimeConfig)"
          )
          assertEquals(echo.cwd, None, "no cwd declared → defaults to the plugin root at runtime (§11.1-7)")
          assertEquals(echo.env, None, "no env declared (zero-credential sample)")
          assertEquals(p.toolsExtension, Nil)
      curlResolved match
        case Left(err) => fail(s"curl-toolkit must resolve: $err")
        case Right(p) =>
          assertEquals(p.toolsExtension, List("Curl"), "closed-form tools.json grants exactly Curl")
          assertEquals(p.skills.map(_.id), List("curl-toolkit/curl-fetch"))
          assertEquals(p.mcpServers, Map.empty)
      assert(catalog.contains("mcp: echo"), s"catalog line must list the mcp server, got:\n$catalog")
      assert(catalog.contains("tools: Curl"), s"catalog line must list the granted tool, got:\n$catalog")
      assert(catalog.contains("- mcp-echo-toolkit:"), s"catalog line for the mcp sample, got:\n$catalog")
      assert(catalog.contains("- curl-toolkit:"), s"catalog line for the tools sample, got:\n$catalog")
  }

  // ── ② 拒答：坏 schema ──────────────────────────────────────

  test("② 坏 schema: 种子副本 plugin.json 非 canonical $schema → 拒载（PLUGIN_SCHEMA_UNSUPPORTED）") {
    freshPlugins()
    val dir = installSeed("mcp-echo-toolkit")
    os.write.over(
      dir / "plugin.json",
      Json
        .obj(
          "$schema" -> Json.fromString("https://agent-plugins.org/schemas/9.9.9/plugin.schema.json"),
          "name" -> Json.fromString("mcp-echo-toolkit"),
          "version" -> Json.fromString("1.0.0"),
          "description" -> Json.fromString("bad schema probe")
        )
        .noSpaces
    )
    PluginRegistry.invalidateCache()
    for
      loaded <- PluginRegistry.scan()
      entry <- rejectedOf("mcp-echo-toolkit")
    yield
      assert(!loaded.exists(_.name == "mcp-echo-toolkit"), "a non-canonical $schema must refuse the package")
      val reason = entry.map(_._2).getOrElse(fail("mcp-echo-toolkit must appear in the rejected list"))
      assert(reason.contains("PLUGIN_SCHEMA_UNSUPPORTED"), s"got: $reason")
  }

  test("② 坏 schema（组件级）: mcp.json 坏 $schema → MCP 组件 invalid，包继续且 skills 照载（§6.2）") {
    freshPlugins()
    val dir = installSeed("mcp-echo-toolkit")
    // mcp-echo-toolkit 原无 skills/——补一个，验证「组件 invalid 不拖垮整包」
    os.makeDir.all(dir / "skills" / "fallback")
    os.write.over(
      dir / "skills" / "fallback" / "SKILL.md",
      """---
        |name: fallback
        |description: keeps the package loadable while its mcp component is invalid
        |---
        |body""".stripMargin
    )
    os.write.over(
      dir / "mcp.json",
      s"""{"$$schema":"https://example.invalid/mcp.schema.json","mcpServers":{"echo":{"type":"stdio","command":"python3"}}}"""
    )
    PluginRegistry.invalidateCache()
    PluginRegistry.scan().map { loaded =>
      loaded.find(_.name == "mcp-echo-toolkit") match
        case None => fail("the package must continue loading when only its mcp.json is invalid (§6.2)")
        case Some(p) =>
          assertEquals(p.mcpServers, Map.empty, "the MCP component must be dropped entirely")
          assertEquals(p.skills.map(_.id), List("mcp-echo-toolkit/fallback"), "skills must keep loading")
          assert(
            p.warnings.exists(w => w.contains("$schema") && w.contains("canonical")),
            s"the component-level invalidation must be reported, got: ${p.warnings}"
          )
    }
  }

  test("② 坏 schema（新规则）: tools.json {\"tools\":\"Curl\"} → 拒载（PLUGIN_TOOLS_SCHEMA，闭合 fail-closed）") {
    freshPlugins()
    val dir = installSeed("curl-toolkit")
    os.write.over(dir / "org.nebflow" / "tools.json", """{"tools":"Curl"}""")
    PluginRegistry.invalidateCache()
    for
      loaded <- PluginRegistry.scan()
      entry <- rejectedOf("curl-toolkit")
    yield
      assert(!loaded.exists(_.name == "curl-toolkit"), "a non-string-array 'tools' must refuse the whole package")
      val reason = entry.map(_._2).getOrElse(fail("curl-toolkit must appear in the rejected list"))
      assert(reason.contains("PLUGIN_TOOLS_SCHEMA"), s"got: $reason")
      assert(reason.contains("'tools'"), s"the reason must name the offending key, got: $reason")
  }

  // ── ③ 拒答：未知键（nebflow 命名空间 fail-closed × 官方件 report-and-ignore）───

  test("③ 未知键: tools.json 加 extra → 拒载（PLUGIN_TOOLS_SCHEMA）；mcp.json entry 未知键 → 告警+忽略+entry 照载") {
    freshPlugins()
    val curlDir = installSeed("curl-toolkit")
    os.write.over(curlDir / "org.nebflow" / "tools.json", """{"tools":["Curl"],"extra":true}""")
    val mcpDir = installSeed("mcp-echo-toolkit")
    os.write.over(
      mcpDir / "mcp.json",
      s"""{"$$schema":"${PluginRegistry.CanonicalMcpSchema}","mcpServers":{"echo":{"type":"stdio","command":"python3","args":["$${PLUGIN_ROOT}/scripts/echo-server.py"],"futureField":42}}}"""
    )
    PluginRegistry.invalidateCache()
    for
      loaded <- PluginRegistry.scan()
      entry <- rejectedOf("curl-toolkit")
    yield
      // nebflow 自有命名空间：未知键 = 拒载（无官方前向兼容义务）
      assert(!loaded.exists(_.name == "curl-toolkit"), "an unknown key in org.nebflow/tools.json must refuse the package")
      val reason = entry.map(_._2).getOrElse(fail("curl-toolkit must appear in the rejected list"))
      assert(reason.contains("PLUGIN_TOOLS_SCHEMA"), s"got: $reason")
      assert(reason.contains("extra"), s"the reason must name the unknown key, got: $reason")
      // 官方件（mcp.json entry）：report-and-ignore 对照臂
      loaded.find(_.name == "mcp-echo-toolkit") match
        case None => fail("the mcp sample must keep loading with an unknown entry field (forward-compat)")
        case Some(p) =>
          assertEquals(p.mcpServers.keySet, Set("echo"), "the entry itself must still load")
          assertEquals(p.mcpServers("echo").command, Some("python3"))
          assert(
            p.warnings.exists(w => w.contains("unknown field 'futureField'")),
            s"the ignored unknown field must be reported (report-and-ignore), got: ${p.warnings}"
          )
  }

  // ── ④ 拒答：前缀违规 ───────────────────────────────────────

  test("④ 前缀违规: 种子副本改名 nebflow-echo-imp → OFFICIAL_IMPERSONATION 拒载") {
    freshPlugins()
    val dir = installSeed("mcp-echo-toolkit", destName = "nebflow-echo-imp")
    os.write.over(
      dir / "plugin.json",
      Json
        .obj(
          "$schema" -> PluginRegistry.CanonicalSchema.asJson,
          "name" -> Json.fromString("nebflow-echo-imp"),
          "version" -> Json.fromString("1.0.0"),
          "description" -> Json.fromString("impersonation probe")
        )
        .noSpaces
    )
    PluginRegistry.invalidateCache()
    for
      loaded <- PluginRegistry.scan()
      entry <- rejectedOf("nebflow-echo-imp")
      resolved <- PluginRegistry.resolve("nebflow-echo-imp")
    yield
      assert(!OfficialPackages.allowlist().contains("nebflow-echo-imp"), "sanity: the impostor is not a shipped package")
      assert(!loaded.exists(_.name == "nebflow-echo-imp"), "a reserved-prefix impostor must not load")
      val reason = entry.map(_._2).getOrElse(fail("nebflow-echo-imp must appear in the rejected list"))
      assert(reason.contains("OFFICIAL_IMPERSONATION"), s"got: $reason")
      assert(
        resolved.isLeft && resolved.swap.toOption.get.contains("PLUGIN_NOT_FOUND"),
        s"a refused package must be unresolvable, got: $resolved"
      )
  }

  // ── ⑤ 跨层口径一致（防漂移，字符串级、跨平台、不 exec python）──────────

  test("⑤ 口径一致: validate_plugin.py RESERVED_PREFIX == OfficialPackages.ReservedPrefix；SKILL.md 无旧口径") {
    // 装载层常量回归锚（官方保留 namespace 逐字）
    assertEquals(OfficialPackages.ReservedPrefix, "nebflow-")
    val py =
      resourceText("seed/plugins/nebflow-plugin-creator/skills/plugin-packaging/scripts/validate_plugin.py")
    assert(
      py.contains("RESERVED_PREFIX = \"" + OfficialPackages.ReservedPrefix + "\""),
      s"validate_plugin.py must pin RESERVED_PREFIX to the loader-side value '${OfficialPackages.ReservedPrefix}' " +
        "(creator-side mirror may not drift to a narrower/older prefix)"
    )
    val skill = resourceText("seed/plugins/nebflow-plugin-creator/skills/plugin-packaging/SKILL.md")
    // 官方包名自身（nebflow-plugin-creator）是旧前缀字符串唯一合法的出现形态——
    // 剥掉后再断言零残留，规则句只允许新口径 `nebflow-`。
    val stripped = skill.replace("nebflow-plugin-creator", "")
    assert(
      !stripped.contains("nebflow-plugin-"),
      "SKILL.md must not carry the retired 'nebflow-plugin-' prefix wording in its rule sentences"
    )
    assert(skill.contains("保留前缀 `nebflow-`"), "the rule sentence must pin the loader-side reserved prefix")
  }

end PluginSeedBridgeSpec
