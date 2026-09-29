package nebflow.gateway

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Json, JsonObject}
import munit.CatsEffectSuite
import nebflow.core.tools.ToolRegistry
import nebflow.ir.*
import nebflow.shared.PathUtil
import nebflow.shared.ContentBlock
import nebflow.shared.{ToolCall, ToolExecResult}

import java.util.concurrent.atomic.AtomicInteger

/**
 * P1-3 活实现（IrLlmRoute）的一致性用例：
 *  - ① llmName 双向唯一：注册期 §7.5 负例（同 llmName 碰撞 / Llm 受众缺 llmName）+
 *    桥表双射（toolName/irName 各自唯一、∈ TOOL_MAP、查表与 registry def 往返一致）；
 *  - ② route miss 清单：非桥接名（AskUserQuestion / $web_search / mcp 前缀名 /
 *    dev:fs 面）与 boot 早期空 registry（fail-open 到旧路径）；
 *  - ③ caps Deny 硬底：显式 Deny 规则与 unknown-cap（fail-closed）两腿 ⇒ Blocked、
 *    活闭包零执行、auto-all 不折 Deny（§8.2）；
 *  - ④ 审计逐节点 tenant=llm（Allow/Ask/Deny 三腿；Ask 腿 exit=null——Router.audit
 *    既有行为面，断言防漂移）；
 *  - ⑤ 活路径富字段旁路保真：frontendContent/imageBlocks 取闭包富值、content 与
 *    闭包模型面串字节相等、isError 以 IR 契约为权威（exit≠0 ⇒ true）；
 *  - ⑥ AskFallback：ConfirmEdits + askRule ⇒ 回退腿、Router 零执行（绝不
 *    await_approval）；
 *  - ⑦ 同批双并发（parTraverse）：IOLocal fiber 隔离——两路各自的闭包/结果零串台
 *    （openQuestions 并发验证项的钉死测试）。
 */
class IrLlmRouteSpec extends CatsEffectSuite:

  private lazy val defs: List[CommandDef] = IrToolBridge.defs(None)

  private var dataRootHome: os.Path = null
  private var prevRoot: os.Path = null

  override def beforeAll(): Unit =
    super.beforeAll()
    prevRoot = PathUtil.dataRoot
    dataRootHome = os.temp.dir(prefix = "nebflow-ir-llmroute-")
    PathUtil.setDataRoot(dataRootHome)

  override def afterAll(): Unit =
    PathUtil.setDataRoot(prevRoot)
    os.remove.all(dataRootHome)
    super.afterAll()

  /** 测试路由装配：真 Router（bridged defs + 可注入规则/词表）+ registry 查表 + 定根/定档。 */
  private def mkRoute(
    h: IrTestKit.Harness,
    safety: nebflow.core.SafetyMode,
    root: os.Path
  ): IrLlmRoute = new IrLlmRoute(
    router = h.router,
    lookupIr = h.router.registry.get,
    rootFor = _ => IO.pure(root.toString),
    safetyOf = IO.pure(safety)
  )

  private def call(name: String, args: (String, Json)*): ToolCall =
    ToolCall(id = s"c-$name", name = name, input = JsonObject(args*))

  /** 计数 + 固定富结果的活闭包（模拟 executeToolInner 的模型面/用户面/图像三域）。 */
  private def richExec(
    counter: AtomicInteger,
    modelFace: String,
    frontend: Option[String] = None,
    images: Option[List[ContentBlock.Image]] = None,
    isError: Boolean = false
  ): IrExec = () =>
    IO.delay(counter.incrementAndGet())
      .as(
        ToolExecResult(modelFace, isError = isError, frontendContent = frontend, imageBlocks = images)
      )

  // ── ① llmName 双向唯一 ─────────────────────────────────────

  test("① 注册期负例：同 trust 档两条 def 同 llmName 且含 Llm 受众 ⇒ N4 外再加 §7.5 uniqueness 拒"):
    IO.delay {
      val reg = new CommandRegistry()
      // 手工构 def（probe 夹具不带 llmName 面）
      def mkDef(name: String, llmName: Option[String]) = CommandDef(
        name = name,
        description = "dup llmName probe (test only)",
        argsSchema = Json.obj("type" -> "object".asJson).asObject.get,
        binding = Binding.Dev((_, _) => IO.pure(Right(StreamValue.Text("x")))),
        io = CommandIo(None, StreamKind.Text),
        caps = Set.empty,
        audiences = Set(Audience.Human, Audience.Llm),
        llmName = llmName
      )
      assertEquals(reg.register(mkDef("dev:test:dup1", Some("Dup"))).isRight, true)
      val dup = reg.register(mkDef("dev:test:dup2", Some("Dup")))
      assertEquals(dup.isLeft, true)
      assertEquals(
        dup.swap.toOption.get.details("reason").flatMap(_.asString),
        Some("llm_name_duplicate")
      )
      val missing = reg.register(mkDef("dev:test:dup3", None))
      assertEquals(missing.isLeft, true)
      assertEquals(
        missing.swap.toOption.get.details("reason").flatMap(_.asString),
        Some("llm_name_missing")
      )
    }

  test("① 桥表双射：toolName/irName 各自唯一、toolName ∈ TOOL_MAP、查表 ↔ registry def 往返一致"):
    IO.delay {
      val rows = IrToolCaps.bridged
      assertEquals(rows.map(_.toolName).distinct.length, rows.length)
      assertEquals(rows.map(_.irName).distinct.length, rows.length)
      rows.foreach { row =>
        assert(ToolRegistry.TOOL_MAP.contains(row.toolName), s"TOOL_MAP must contain ${row.toolName}")
        assertEquals(IrLlmRoute.llmNameToIr.get(row.toolName), Some(row.irName))
      }
      // registry def 侧：irName → llmName == toolName（往返一致）
      val reg = new CommandRegistry()
      DevCommands.base.foreach(c => reg.register(c))
      defs.foreach(c => reg.register(c).fold(e => fail(s"register ${c.name}: ${e.message}"), _ => ()))
      rows.foreach { row =>
        reg.get(row.irName) match
          case Some(d) =>
            assertEquals(d.llmName, Some(row.toolName), s"llmName of ${row.irName}")
            assert(d.audiences.contains(Audience.Llm), s"llm audience of ${row.irName} (P1-3 flip)")
          case None => fail(s"registry missing ${row.irName}")
      }
    }

  // ── ② route miss ──────────────────────────────────────────

  test("② route miss 清单：非桥接名零咨询；boot 早期空 registry ⇒ fail-open 旧路径"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root, rules = IrToolCaps.askRules, extra = defs).flatMap { h =>
        val route = mkRoute(h, nebflow.core.SafetyMode.AutoAll, root)
        val counter = new AtomicInteger(0)
        val exec = richExec(counter, "nope")
        val misses = List("AskUserQuestion", "$web_search", "mcp__srv__tool", "dev:fs:ls", "TotallyUnknown")
        misses.traverse_ { name =>
          IO.delay {
            assertEquals(route.route(call(name), "s", "r", "a", exec), None, s"route miss expected for $name")
          }
        } *> IO.delay(assertEquals(counter.get, 0))
      }
    }

  test("② boot 早期：桥接名但 registry 未注册 ⇒ None（fail-open 到 off 态）"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val route = mkRoute(h, nebflow.core.SafetyMode.AutoAll, root)
        val exec = richExec(new AtomicInteger(0), "nope")
        assertEquals(
          route.route(call("Read", "file_path" -> "/etc/hosts".asJson), "s", "r", "a", exec).isDefined,
          false
        )
        IO.unit
      }
    }

  // ── ③ caps Deny 硬底 + ⑤ 活路径保真 + ④ 审计 ───────────────

  test("③⑤ Allow 腿：AutoAll 折 askRule 的 Ask ⇒ Executed；content 与闭包模型面串字节相等；frontendContent/imageBlocks 旁路保真"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root, rules = IrToolCaps.askRules, extra = defs).flatMap { h =>
        val route = mkRoute(h, nebflow.core.SafetyMode.AutoAll, root)
        val counter = new AtomicInteger(0)
        val img = ContentBlock.Image("aGVsbG8=", "image/png")
        val exec = richExec(
          counter,
          modelFace = "model-face-string",
          frontend = Some("frontend-face-string"),
          images = Some(List(img))
        )
        route
          .route(call("Read", "file_path" -> (root / "a.txt").toString.asJson), "sess-x", "req-x", "agent-x", exec)
          .get
          .map {
            case IrRouteLeg.Executed(r) =>
              assertEquals(r.isError, false)
              assertEquals(counter.get, 1) // 活闭包恰执行一次（经 IR 全链）
              assertEquals(r.content, "model-face-string") // 字节相等（IR 契约面 = 闭包模型面）
              assertEquals(r.frontendContent, Some("frontend-face-string")) // 旁路保真（非 Some(stdout) 基线）
              assertEquals(r.imageBlocks, Some(List(img))) // 图像旁路保真（纯映射恒 None 的活路径例外）
            case other => fail(s"expected Executed, got $other")
          }
      }
    }

  test("③⑤ 失败腿：闭包 isError=true ⇒ exit≠0 ⇒ IR 契约 isError=true、content=闭包错误串"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root, rules = IrToolCaps.askRules, extra = defs).flatMap { h =>
        val route = mkRoute(h, nebflow.core.SafetyMode.AutoAll, root)
        val exec = richExec(new AtomicInteger(0), modelFace = "tool blew up", isError = true)
        route
          .route(call("Read", "file_path" -> (root / "a.txt").toString.asJson), "sess-x", "req-x", "agent-x", exec)
          .get
          .map {
            case IrRouteLeg.Executed(r) =>
              assertEquals(r.isError, true) // exit≠0 ⇒ isError=true（契约新做）
              assertEquals(r.content, "command.failed: tool blew up") // commandFailed(message=modelFace)
              assertEquals(r.frontendContent, None)
            case other => fail(s"expected Executed, got $other")
          }
      }
    }

  test("③ Deny 硬底：显式 Deny 规则 ⇒ Blocked、零执行、isError=true、content 含 policy.denied 与 rule；auto-all 不折 Deny"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit
        .harness(
          root,
          rules = PolicyRule("dev:tool:read", Decision.Deny("no reads from llm", "test:deny")) :: IrToolCaps.askRules,
          extra = defs
        )
        .flatMap { h =>
          val route = mkRoute(h, nebflow.core.SafetyMode.AutoAll, root)
          val counter = new AtomicInteger(0)
          route
            .route(
              call("Read", "file_path" -> "/etc/hosts".asJson),
              "sess-x",
              "req-x",
              "agent-x",
              richExec(counter, "x")
            )
            .get
            .map {
              case IrRouteLeg.Blocked(r) =>
                assertEquals(r.isError, true)
                assert(r.content.contains("policy.denied"), r.content)
                assert(r.content.contains("rule=test:deny"), r.content)
                assertEquals(counter.get, 0) // 零执行
              case other => fail(s"expected Blocked, got $other")
            }
        }
    }

  test("③ unknown-cap fail-closed：词表外 cap ⇒ Deny（[I7] 保证零执行），auto-all 亦不折"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root, knownCapKinds = Set.empty, extra = defs).flatMap { h =>
        val route = mkRoute(h, nebflow.core.SafetyMode.AutoAll, root)
        val counter = new AtomicInteger(0)
        route
          .route(call("Read", "file_path" -> "/etc/hosts".asJson), "sess-x", "req-x", "agent-x", richExec(counter, "x"))
          .get
          .map {
            case IrRouteLeg.Blocked(r) =>
              assertEquals(r.isError, true)
              assert(r.content.contains("fail-closed:unknown-cap"), r.content)
              assertEquals(counter.get, 0)
            case other => fail(s"expected Blocked, got $other")
          }
      }
    }

  test("⑥ AskFallback：ConfirmEdits + askRule ⇒ 回退腿、Router 零执行（绝不 await_approval）"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root, rules = IrToolCaps.askRules, extra = defs).flatMap { h =>
        val route = mkRoute(h, nebflow.core.SafetyMode.ConfirmEdits, root)
        val counter = new AtomicInteger(0)
        route
          .route(call("Read", "file_path" -> "/etc/hosts".asJson), "sess-x", "req-x", "agent-x", richExec(counter, "x"))
          .get
          .map(leg => assertEquals(leg, IrRouteLeg.AskFallback))
          *> IO.delay(assertEquals(counter.get, 0))
      }
    }

  test("④ 审计逐节点 tenant=llm：Allow/Ask/Deny 三腿各一条，tenant/ingress/node/command/decision 齐；Ask 腿 exit=null"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit
        .harness(
          root,
          rules = List(
            PolicyRule("dev:tool:write", Decision.Deny("no", "test:deny-write")),
            PolicyRule("dev:tool:read", Decision.Allow)
          ),
          extra = defs
        )
        .flatMap { h =>
          val route = mkRoute(h, nebflow.core.SafetyMode.ConfirmEdits, root)
          val exec = richExec(new AtomicInteger(0), "ok")
          val allow =
            route.route(call("Read", "file_path" -> "/etc/hosts".asJson), "sess-a", "req-a", "agent-a", exec).get
          val deny = route
            .route(
              call("Write", "file_path" -> "/tmp/x".asJson, "content" -> "x".asJson),
              "sess-a",
              "req-b",
              "agent-a",
              exec
            )
            .get
          val ask = route.route(call("Glob", "pattern" -> "*.txt".asJson), "sess-a", "req-c", "agent-a", exec).get
          for
            l <- allow
            d <- deny
            a <- ask
            records <- h.records
          yield
            assertEquals(l.isInstanceOf[IrRouteLeg.Executed], true)
            assertEquals(d.isInstanceOf[IrRouteLeg.Blocked], true)
            assertEquals(a, IrRouteLeg.AskFallback)
            assertEquals(records.length, 3)
            records.foreach { rec =>
              assertEquals(rec.tenant, "llm:sess-a/agent-a")
              assertEquals(rec.ingress, "llm")
              assertEquals(rec.node, "0")
              assert(rec.command.startsWith("dev:tool:"), rec.command) // IR 名只活在审计 command 字段
            }
            val byDecision = records.map(r => r.decision -> r).toMap
            assertEquals(byDecision("allow").command, "dev:tool:read")
            assertEquals(byDecision("deny").command, "dev:tool:write")
            assertEquals(byDecision("deny").rule, Some("test:deny-write"))
            assertEquals(byDecision("ask").command, "dev:tool:glob")
            assertEquals(byDecision("ask").exit, None) // Ask 腿零执行 ⇒ exit=null（§8.6）
          end for
        }
    }

  test("⑦ 同批双并发：parTraverse 两路各自闭包/结果零串台（IOLocal fiber 隔离钉死）"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root, rules = IrToolCaps.askRules, extra = defs).flatMap { h =>
        val route = mkRoute(h, nebflow.core.SafetyMode.AutoAll, root)
        val c1 = new AtomicInteger(0)
        val c2 = new AtomicInteger(0)
        val e1 = richExec(c1, "alpha-result", frontend = Some("alpha-front"))
        val e2 = richExec(c2, "beta-result", frontend = Some("beta-front"))
        val legs = List(
          route.route(call("Read", "file_path" -> (root / "a.txt").toString.asJson), "s", "r1", "a", e1).get,
          route.route(call("Grep", "pattern" -> "x".asJson), "s", "r2", "a", e2).get
        ).parSequence
        legs.map {
          case List(IrRouteLeg.Executed(r1), IrRouteLeg.Executed(r2)) =>
            assertEquals(r1.content, "alpha-result")
            assertEquals(r1.frontendContent, Some("alpha-front"))
            assertEquals(r2.content, "beta-result")
            assertEquals(r2.frontendContent, Some("beta-front"))
            assertEquals(c1.get, 1)
            assertEquals(c2.get, 1)
          case other => fail(s"expected two Executed legs, got $other")
        }
      }
    }

  test("配置键：ir.llmIngress 缺省/非法 fail-safe 关，显式 true 才开；NebflowServiceConfig 无 ir 节照常解码"):
    IO.delay {
      assertEquals(IrLlmRoute.llmIngressEnabled(None), false)
      assertEquals(IrLlmRoute.llmIngressEnabled(Some(Json.obj())), false)
      assertEquals(IrLlmRoute.llmIngressEnabled(Some(Json.obj("llmIngress" -> "yes".asJson))), false)
      assertEquals(IrLlmRoute.llmIngressEnabled(Some(Json.obj("llmIngress" -> true.asJson))), true)
      assertEquals(IrLlmRoute.llmIngressEnabled(Some(Json.obj("llmIngress" -> false.asJson))), false)
      // 既有安装（无 ir 节）照常解码为 None；显式节点解码后经同一 fail-safe 面取值
      import io.circe.parser.*
      val legacy = parse("""{"llm":{"providers":{}}}""").flatMap(_.as[nebflow.shared.NebflowServiceConfig])
      assertEquals(legacy.map(_.ir), Right(Option.empty[io.circe.Json]))
      val on = parse("""{"llm":{"providers":{}},"ir":{"llmIngress":true}}""")
        .flatMap(_.as[nebflow.shared.NebflowServiceConfig])
      assertEquals(
        on.map(c => c.ir.exists(j => IrLlmRoute.llmIngressEnabled(Some(j)))),
        Right(true): Either[io.circe.Error, Boolean]
      )
    }

end IrLlmRouteSpec
