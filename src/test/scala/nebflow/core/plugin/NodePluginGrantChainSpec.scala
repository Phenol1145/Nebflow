package nebflow.core.plugin

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import munit.CatsEffectSuite
import nebflow.actor.ActorSystem
import nebflow.agent.{AgentLibrary, SharedResources}
import nebflow.core.RateLimiter
import nebflow.core.project.{
  FlowMapStore,
  NodeDef,
  NodeEngine,
  NodeLifecycle,
  ProjectDef,
  ProjectRuntime,
  ProjectRuntimeRegistry
}
import nebflow.core.task.FileTaskStore
import nebflow.core.tools.{FileLockManager, NodeEditTool, ToolContext}
import nebflow.llm.ModelCandidate
import nebflow.shared.{LlmHandle, LlmRequest, LlmResponse, PathUtil, StreamChunk, ThinkingConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*

/**
 * 种子桥批（2026-09-30）——官方种子样例包的**授予生效链** spec
 * （NodePluginChainSpec 同款 RecordingLlm 基建；fixture = seed 树字节副本）。
 *
 * - ① tools 授予链：node.plugins=[curl-toolkit 种子副本] → NodeEdit 受理（在位即
 *   信任）→ spawn → 首条 LlmRequest.tools 含 "Curl" 且不含 "WebSearch"
 *   （NodeStarter.prepareNodePlugins → buildAllowedToolSet 白名单二次过滤后追加）
 *   → 节点 Completed → node.plugins 留痕；
 * - ② MCP 授予+回收链：node.plugins=[mcp-echo-toolkit 种子副本]（mcp.json 的
 *   ${PLUGIN_ROOT} args 占位符在 acquire 期展开，真 python3 stdio server）→ 工具
 *   `mcp__plugin_mcp-echo-toolkit_echo__echo` 注册进会话清单 → refcount=1 → 终态后
 *   工具注销 + server 摘除（NodeStarter.guarantee(release) 全终态汇合点）。
 */
class NodePluginGrantChainSpec extends CatsEffectSuite:

  override val munitIOTimeout: FiniteDuration = 180.seconds

  private val tempRoot: os.Path = os.pwd / "target" / "test-plugin-grant-chain"
  private val originalRoot = PathUtil.dataRoot

  PathUtil.setDataRoot(tempRoot)
  os.remove.all(tempRoot)
  os.makeDir.all(tempRoot / "agents" / "test-agent")

  os.write.over(
    tempRoot / "agents" / "test-agent" / "agent.json",
    """{"name":"test-agent","description":"grant chain agent","tools":[],"category":"standalone"}"""
  )
  os.write.over(tempRoot / "agents" / "test-agent" / "system.md", "# test-agent\n")
  // 节点执行统一 general（2026-09-05 agent 退役）——fixture 侧补 general agent
  os.makeDir.all(tempRoot / "agents" / "general")

  os.write.over(
    tempRoot / "agents" / "general" / "agent.json",
    """{"name":"general","description":"general executor","tools":[],"category":"standalone"}"""
  )
  os.write.over(tempRoot / "agents" / "general" / "system.md", "# general\n")
  os.write.over(tempRoot / "nebflow.json", "{}")

  // ── fixtures：官方种子样例包的字节副本（classpath → 隔离 plugins/）────────

  private def seedDir(name: String): os.Path =
    val url = getClass.getClassLoader.getResource(s"seed/plugins/$name/plugin.json")
    assert(url != null, s"seed/plugins/$name not on the test classpath")
    os.Path(java.nio.file.Paths.get(url.toURI)) / os.up

  List("curl-toolkit", "mcp-echo-toolkit").foreach { name =>
    os.copy(
      seedDir(name),
      tempRoot / "plugins" / name,
      createFolders = true,
      mergeFolders = true,
      replaceExisting = true
    )
  }

  override def afterAll(): Unit =
    PathUtil.setDataRoot(originalRoot)

  // ── 基建（NodePluginChainSpec 同款）─────────────────────────

  private class RecordingLlm(capture: TrieMap[String, LlmRequest], delay: FiniteDuration = Duration.Zero)
      extends LlmHandle[IO]:
    def send(req: LlmRequest): IO[LlmResponse] = IO.raiseError(new RuntimeException("send not expected"))

    def sendStream(
      req: LlmRequest,
      onAttempt: Option[nebflow.shared.FallbackAttempt => IO[Unit]] = None
    ): Stream[IO, StreamChunk] =
      val put = IO(capture.update(req.sessionId, req)).void
      val body: Stream[IO, StreamChunk] = Stream(StreamChunk.TextDelta("ok"), StreamChunk.Done(None, None))
      val out: Stream[IO, StreamChunk] =
        if delay == Duration.Zero then Stream.eval(put).drain ++ body
        else (Stream.eval(IO.sleep(delay)) ++ Stream.eval(put)).drain ++ body
      out

  private def mkResources(system: ActorSystem, tmp: os.Path, llm: LlmHandle[IO]): IO[SharedResources] =
    for
      dispatcher <- cats.effect.std.Dispatcher.parallel[IO].allocated.map(_._1)
      rateLimiter <- RateLimiter.create()
      tracker <- nebflow.core.FileChangeTracker.create(os.pwd.toString)
      fileLocks <- FileLockManager.create
      thinkingRef <- Ref.of[IO, ThinkingConfig](ThinkingConfig())
      modelOverrides <- Ref.of[IO, Map[String, ModelCandidate]](Map.empty)
      voiceMuted <- Ref.of[IO, Boolean](false)
    yield SharedResources(
      llm = llm,
      dispatcher = dispatcher,
      sessionStore = nebflow.core.SessionStore(tmp / "sessions", tmp / "tasks"),
      projectRoot = os.pwd,
      thinkingConfigRef = thinkingRef,
      rateLimiter = rateLimiter,
      fileChangeTracker = tracker,
      contextWindow = 100_000,
      agentLibrary = new AgentLibrary(tmp / "agents"),
      taskStore = FileTaskStore,
      historyArchiver = null,
      fileLockManager = fileLocks,
      sessionModelOverrides = modelOverrides,
      providerRegistry = null,
      healthMonitor = null,
      actorSystem = null,
      voiceMutedRef = voiceMuted
    )

  private def mkCtx(res: SharedResources, system: ActorSystem, ws: String): ToolContext =
    ToolContext(
      projectRoot = ws,
      sessionId = Some("grant-sid"),
      rootSessionId = Some("nebula-root"),
      sharedResources = Some(res),
      actorSystem = Some(system)
    )

  private def mountProject(name: String, ws: os.Path, system: ActorSystem, res: SharedResources): IO[ProjectRuntime] =
    for
      store <- FlowMapStore.open(name, ws.toString)
      engine = new NodeEngine(
        store,
        system,
        res,
        wsSendFn = (_: Json) => IO.unit,
        workspace = ws.toString,
        rootSessionId = "nebula-root",
        projectName = name,
        emitEvent = (_, _, _) => IO.unit,
        reportGateHold = Some(false)
      )
      pd = ProjectDef(
        name = name,
        workspace = ws.toString,
        agentFile = (ws / "AGENTS.md").toString,
        createdAt = System.currentTimeMillis()
      )
      rt = ProjectRuntime(pd, store, engine, system, res, None)
      _ <- ProjectRuntimeRegistry.register(rt)
    yield rt

  private def nodeEdit(input: Json, ctx: ToolContext): IO[Either[String, String]] =
    NodeEditTool.call(input.asObject.get, ctx).map(_.left.map(_.message))

  private def nodeInput(project: String, nodename: String, extra: (String, Json)*): Json =
    Json.obj(
      ("project" -> Json
        .fromString(project)) :: ("nodename" -> Json.fromString(nodename)) :: ("plugins" -> Json.arr()) :: extra.toList*
    )

  private def waitUntil(timeout: FiniteDuration, every: FiniteDuration = 50.millis)(cond: IO[Boolean]): IO[Unit] =
    def go(deadline: Long): IO[Unit] =
      cond.flatMap {
        case true => IO.unit
        case false =>
          if System.currentTimeMillis() >= deadline then
            IO.raiseError(new AssertionError("waitUntil: condition not met in time"))
          else IO.sleep(every) *> go(deadline)
      }
    go(System.currentTimeMillis() + timeout.toMillis)

  // ── ① tools 授予链 ─────────────────────────────────────────

  test("① tools 授予: node.plugins=[curl-toolkit] → 首条 LlmRequest.tools 含 Curl 不含 WebSearch → Completed + 留痕") {
    val capture = TrieMap[String, LlmRequest]()
    val ws = tempRoot / "ws-grant-curl"
    os.makeDir.all(ws)
    val system = ActorSystem(s"grant-curl-${scala.util.Random.nextInt(100000)}")
    val program =
      for
        res <- mkResources(system, tempRoot, new RecordingLlm(capture))
        rt <- mountProject("grant-curl", ws, system, res)
        ctx = mkCtx(res, system, ws.toString)
        created <- nodeEdit(
          nodeInput(
            "grant-curl",
            "curled",
            "description" -> Json.fromString("test node purpose"),
            "task" -> Json.fromString("use curl for a raw http probe"),
            "out" -> Json.fromString("Nebula"),
            "plugins" -> Json.arr(Json.fromString("curl-toolkit"))
          ),
          ctx
        )
        _ = assert(created.isRight, s"NodeEdit with the curl-toolkit seed copy must be accepted: $created")
        _ <- waitUntil(30.seconds)(
          rt.store.snapshot.map(_.nodes.values.exists(n => n.name == "curled" && n.status == NodeLifecycle.Completed))
        )
        node <- rt.store.snapshot.map(_.nodes.values.find(_.name == "curled")).flatMap {
          case Some(n) => IO.pure(n)
          case None => IO.raiseError(new RuntimeException("node vanished"))
        }
        _ <- system.stopAll.handleErrorWith(_ => IO.unit)
      yield (node, capture.values.find(_.sessionId.startsWith("node-")))
    program.map { case (node, reqOpt) =>
      val req = reqOpt.getOrElse(fail("no node LLM request captured"))
      val toolNames = req.tools.getOrElse(Nil).map(_.name)
      assert(toolNames.contains("Curl"), s"the granted builtin tool must be in the session tool list, got: ${toolNames.mkString(",")}")
      assert(
        !toolNames.contains("WebSearch"),
        s"curl-toolkit grants only Curl — WebSearch must stay out, got: ${toolNames.mkString(",")}"
      )
      assertEquals(node.plugins, List("curl-toolkit"), "node model must record the allocation")
      assertEquals(node.status, NodeLifecycle.Completed)
    }
  }

  // ── ② MCP 授予 + 回收链 ────────────────────────────────────

  test("② MCP 授予+回收: node.plugins=[mcp-echo-toolkit] → echo 工具进会话清单 → refcount=1 → 终态注销+摘除") {
    val capture = TrieMap[String, LlmRequest]()
    val ws = tempRoot / "ws-grant-mcp"
    os.makeDir.all(ws)
    val system = ActorSystem(s"grant-mcp-${scala.util.Random.nextInt(100000)}")
    // 慢 LLM（1.5s）：拉住运行中态，refcount/注册中间态可断言（NodePluginChainSpec 同款）
    val toolPrefix = "mcp__plugin_mcp-echo-toolkit_echo__"
    val serverId = "plugin_mcp-echo-toolkit_echo"
    val program =
      for
        res <- mkResources(system, tempRoot, new RecordingLlm(capture, delay = 1500.millis))
        rt <- mountProject("grant-mcp", ws, system, res)
        ctx = mkCtx(res, system, ws.toString)
        created <- nodeEdit(
          nodeInput(
            "grant-mcp",
            "mcped",
            "description" -> Json.fromString("test node purpose"),
            "task" -> Json.fromString("use the echo tool"),
            "out" -> Json.fromString("Nebula"),
            "plugins" -> Json.arr(Json.fromString("mcp-echo-toolkit"))
          ),
          ctx
        )
        _ = assert(created.isRight, s"NodeEdit with the mcp-echo-toolkit seed copy must be accepted: $created")
        // ${PLUGIN_ROOT} 展开后的真 python3 stdio server 起动 → echo 工具注册
        _ <- waitUntil(30.seconds)(
          IO.blocking(nebflow.core.tools.ToolRegistry.ALL_TOOLS.map(_.name)).map(_.exists(_.startsWith(toolPrefix)))
        )
        _ <- waitUntil(30.seconds)(res.pluginMcp.runningServers.map(_.get(serverId).contains(1)))
        sessionRunning <- res.pluginMcp.runningServers
        _ <- waitUntil(30.seconds)(
          rt.store.snapshot.map(_.nodes.values.exists(n => n.name == "mcped" && n.status == NodeLifecycle.Completed))
        )
        _ <- waitUntil(30.seconds)(res.pluginMcp.sessionHolds.map(_.isEmpty))
        // 回收链确定性终点：release 先清 refs，stopServer（注销+进程关闭）其后异步落定
        _ <- waitUntil(30.seconds)(
          IO.blocking(nebflow.core.tools.ToolRegistry.ALL_TOOLS.map(_.name)).map(!_.exists(_.startsWith(toolPrefix)))
        )
        _ <- waitUntil(30.seconds)(res.pluginMcp.runningServers.map(!_.contains(serverId)))
        toolsAfter <- IO.blocking(nebflow.core.tools.ToolRegistry.ALL_TOOLS.map(_.name))
        runningAfter <- res.pluginMcp.runningServers
        _ <- system.stopAll.handleErrorWith(_ => IO.unit)
      yield (sessionRunning, toolsAfter, runningAfter, capture.values.find(_.sessionId.startsWith("node-")))
    program.map { case (sessionRunning, toolsAfter, runningAfter, reqOpt) =>
      assertEquals(sessionRunning.get(serverId), Some(1), "refcount must be 1 while the node session runs")
      val req = reqOpt.getOrElse(fail("no node LLM request captured"))
      val toolNames = req.tools.getOrElse(Nil).map(_.name)
      assert(
        toolNames.exists(_.startsWith(toolPrefix)),
        s"the plugin MCP tool must be in the session tool list, got: ${toolNames.mkString(",")}"
      )
      assert(
        !toolsAfter.exists(_.startsWith(toolPrefix)),
        "plugin MCP tools must be unregistered after the node terminal state (recycle)"
      )
      assert(!runningAfter.contains(serverId), "the server must be stopped after the terminal recycle")
    }
  }

end NodePluginGrantChainSpec
