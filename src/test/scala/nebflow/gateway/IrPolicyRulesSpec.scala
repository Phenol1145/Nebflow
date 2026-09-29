package nebflow.gateway

import cats.effect.IO
import cats.syntax.all.*
import io.circe.JsonObject
import munit.CatsEffectSuite
import nebflow.ir.*
import nebflow.shared.PathUtil

import java.nio.file.Files

/**
 * P1-4（策略表数据化）一致性用例：`nebflow.json` 顶层 `ir.policy.rules` 的解码全口子
 * （含 v2 评审补齐的 policy 节级 / 重复匹配键）+ 合成序 + denyAll fail-closed +
 * `router.submit(policyOverride)` 端到端 + 缺省路径逐字节等价。
 *
 * dataRoot 隔离（GlobalSafetySpec 同款 beforeEach/afterEach PathUtil.setDataRoot）：
 * [[IrPolicyRules.load]] 每访问热读 dataRoot 下的 nebflow.json。
 *
 * 注：文件落 gateway 包（方案原路径 nebflow/ir/ 的偏差）——IrPolicyRules 是
 * private[gateway]，nebflow.ir 包内的 spec 看不见它；纯解码面（PolicyRule.decodeConfig*）
 * 本身在 ir/Policy.scala，网关包可直接调用。
 */
class IrPolicyRulesSpec extends CatsEffectSuite:

  private var savedRoot: os.Path = null
  private var tmp: os.Path = null

  override def beforeEach(context: BeforeEach): Unit =
    savedRoot = PathUtil.dataRoot
    tmp = os.Path(Files.createTempDirectory("nb-irpolicy"))
    PathUtil.setDataRoot(tmp)

  override def afterEach(context: AfterEach): Unit =
    PathUtil.setDataRoot(savedRoot)
    os.remove.all(tmp)

  private def writeConfig(content: String): Unit =
    os.write.over(tmp / "nebflow.json", content)

  private def policyCfg(rules: String): String = s"""{"ir":{"policy":{"rules":[$rules]}}}"""

  /** 基线引擎：通配 Ask 罩 dev:test:*（用户精确规则的先中/遮蔽载体）。 */
  private def baseEngine: PolicyEngine = new PolicyEngine(
    PolicyConfig(rules = List(PolicyRule("dev:test:*", Decision.Ask("baseline ask", "base:ask"))))
  )

  private def submit(h: IrTestKit.Harness, plan: Ir, safety: Safety, view: Option[PolicyEngine]): IO[PlanResult] =
    h.router.submit(IrTestKit.req(plan), VfsRoot(h.root), safety, view)

  private def targetOf(command: String, caps: Set[Cap]): NodeTarget =
    NodeTarget(
      node = "0",
      command = command,
      caps = caps,
      args = JsonObject.empty,
      pathArgs = Map.empty,
      binding = None
    )

  // ── ① 合法规则端到端：三态 + 审计 rule 名 = 配置 rule 串 ───────────────

  test("① 合法规则端到端：allow 执行 / ask ⇒ await_approval / deny ⇒ exit 125，审计 rule=配置串"):
    IrTestKit.vfs().flatMap { root =>
      writeConfig(
        policyCfg(
          """{"name":"dev:test:probe","decision":"allow"}""" + "," +
            """{"name":"dev:test:sink","decision":"ask","reason":"needs a human","rule":"cfg:ask"}""" + "," +
            """{"name":"dev:test:boom","decision":"deny","reason":"never boom","rule":"cfg:deny"}"""
        )
      )
      IrTestKit.harness(root).flatMap { h =>
        for
          view <- IrPolicyRules.view(h.policy)
          rAllow <- submit(h, IrTestKit.call("dev:test:probe"), Safety.ConfirmEdits, Some(view))
          rAsk <- submit(h, IrTestKit.call("dev:test:sink"), Safety.ConfirmEdits, Some(view))
          rDeny <- submit(h, IrTestKit.call("dev:test:boom"), Safety.ConfirmEdits, Some(view))
          recs <- h.records
          counter <- h.counter.get
        yield
          assertEquals(rAllow.status, Status.Done)
          assertEquals(counter, 1)
          assertEquals(rAsk.status, Status.AwaitApproval)
          assertEquals(rAsk.approval.map(_.rule), Some("cfg:ask"))
          assertEquals(rDeny.status, Status.Rejected)
          assertEquals(rDeny.exit, Some(125))
          assertEquals(rDeny.error.map(_.code), Some(Codes.PolicyDenied))
          assertEquals(rDeny.error.flatMap(_.details("rule").flatMap(_.asString)), Some("cfg:deny"))
          val deny = recs.find(r => r.command == "dev:test:boom" && r.decision == "deny")
          assertEquals(deny.flatMap(_.rule), Some("cfg:deny"))
        end for
      }
    }

  // ── ② 合成序：用户前置首匹配先中；用户 Deny 是 auto-all 也翻不过的硬底 ─────

  test("② 合成序：用户精确 Allow 先中于基线通配 Ask；用户 Deny 在 AutoAll 下仍 Rejected"):
    IrTestKit.vfs().flatMap { root =>
      writeConfig(policyCfg("""{"name":"dev:test:probe","decision":"allow"}"""))
      IrTestKit.harness(root).flatMap { h =>
        for
          vAllow <- IrPolicyRules.view(baseEngine)
          rAllow <- h.router.submit(
            IrTestKit.req(IrTestKit.call("dev:test:probe")),
            VfsRoot(root),
            Safety.ConfirmEdits,
            Some(vAllow)
          )
          _ = writeConfig(policyCfg("""{"name":"dev:test:probe","decision":"deny","reason":"no","rule":"cfg:deny"}"""))
          vDeny <- IrPolicyRules.view(baseEngine)
          rDeny <- h.router.submit(
            IrTestKit.req(IrTestKit.call("dev:test:probe")),
            VfsRoot(root),
            Safety.AutoAll,
            Some(vDeny)
          )
        yield
          // 用户 Allow 先中 ⇒ 基线通配 Ask 不再命中（若合成序反了会是 await_approval）
          assertEquals(rAllow.status, Status.Done)
          // 用户 Deny 硬底：auto-all 只折 Ask，不折 Deny（composeSafety 既有语义零动）
          assertEquals(rDeny.status, Status.Rejected)
          assertEquals(rDeny.exit, Some(125))
          assertEquals(rDeny.error.flatMap(_.details("rule").flatMap(_.asString)), Some("cfg:deny"))
        end for
      }
    }

  // ── ③ fail-closed 全口子：任一非法 ⇒ 整配置拒 ⇒ 全 Deny，绝不静默回落基线 ──

  /** AutoAll 下唯一能 Rejected 的路径就是 denyAll——负腿同时证明没有静默回落。 */
  private def assertRejectedConfig(config: String): IO[Unit] =
    writeConfig(config)
    IrPolicyRules.load.flatMap { loaded =>
      assertEquals(loaded.isLeft, true, s"expected Left for: $config")
      IrPolicyRules.view(baseEngine).flatMap { view =>
        IrTestKit.vfs().flatMap { root =>
          IrTestKit.harness(root).flatMap { h =>
            submit(h, IrTestKit.call("dev:test:probe"), Safety.AutoAll, Some(view)).map { r =>
              assertEquals(r.status, Status.Rejected)
              assertEquals(r.exit, Some(125))
              assertEquals(r.error.map(_.code), Some(Codes.PolicyDenied))
              assertEquals(r.error.flatMap(_.details("rule").flatMap(_.asString)), Some("policy:config-rejected"))
            }
          }
        }
      }
    }

  end assertRejectedConfig

  test("③ fail-closed 节级/rule 级全口子：⇒ Left + 全 Deny(rule=policy:config-rejected)"):
    val overLimit = (1 to 257).map(i => s"""{"name":"a:r$i","decision":"allow"}""").mkString(",")
    val bad = List(
      ("policy 非对象(串)", """{"ir":{"policy":"nope"}}"""),
      ("policy 非对象(数组)", """{"ir":{"policy":[1]}}"""),
      ("policy 未知键", """{"ir":{"policy":{"rulez":[]}}}"""),
      ("policy 无 rules 键", """{"ir":{"policy":{}}}"""),
      ("rules 为 null", """{"ir":{"policy":{"rules":null}}}"""),
      ("rules 非数组", """{"ir":{"policy":{"rules":{"name":"x"}}}}"""),
      ("rule 非对象", """{"ir":{"policy":{"rules":["x"]}}}"""),
      ("rule 未知键", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"allow","extra":1}]}}}"""),
      ("name 缺席", """{"ir":{"policy":{"rules":[{"decision":"allow"}]}}}"""),
      ("decision 坏值", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"maybe"}]}}}"""),
      ("ask 缺 reason", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"ask","rule":"r"}]}}}"""),
      ("ask 缺 rule", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"ask","reason":"r"}]}}}"""),
      ("deny 缺 reason", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"deny","rule":"r"}]}}}"""),
      ("deny 缺 rule", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"deny","reason":"r"}]}}}"""),
      ("allow 带 reason", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"allow","reason":"r"}]}}}"""),
      ("allow 带 rule", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"allow","rule":"r"}]}}}"""),
      ("reason 为 null", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"deny","reason":null,"rule":"r"}]}}}"""),
      ("name 中段 star", """{"ir":{"policy":{"rules":[{"name":"a:*b","decision":"allow"}]}}}"""),
      ("name 双 star", """{"ir":{"policy":{"rules":[{"name":"a:b**","decision":"allow"}]}}}"""),
      ("name 大写段", """{"ir":{"policy":{"rules":[{"name":"A:b","decision":"allow"}]}}}"""),
      ("name 单段", """{"ir":{"policy":{"rules":[{"name":"probe","decision":"allow"}]}}}"""),
      ("name 段超长", s"""{"ir":{"policy":{"rules":[{"name":"a:${"x" * 33}","decision":"allow"}]}}}"""),
      (
        "name 总长超限",
        s"""{"ir":{"policy":{"rules":[{"name":"ext:${("y" * 40) + ":" + ("z" * 40)}","decision":"allow"}]}}}"""
      ),
      ("未知 capKind", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"allow","capKinds":["Telepathy"]}]}}}"""),
      ("capKinds 非数组", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"allow","capKinds":"Exec"}]}}}"""),
      ("capKinds 非串项", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"allow","capKinds":[3]}]}}}"""),
      ("tenant 坏值", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"allow","tenant":"robot"}]}}}"""),
      ("tenant 为 null", """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"allow","tenant":null}]}}}"""),
      (
        "重复匹配键",
        """{"ir":{"policy":{"rules":[{"name":"a:b","decision":"allow"},{"name":"a:b","decision":"deny","reason":"r","rule":"r"}]}}}"""
      ),
      (">256 条", s"""{"ir":{"policy":{"rules":[$overLimit]}}}"""),
      ("文件不可解析", """{"ir":""")
    )
    bad.traverse_ { case (_, cfg) => assertRejectedConfig(cfg) }

  test("③b 读盘 IO 失败 ⇒ Left(不抛)"):
    os.makeDir.all(tmp / "nebflow.json") // 读路径成目录 ⇒ os.read 抛 ⇒ handleErrorWith ⇒ Left
    IrPolicyRules.load.map(loaded => assertEquals(loaded.isLeft, true))

  test("③c denyAll 在 decide 内最先于 unknown-cap（缩词表基线 + 被拒配置 ⇒ config-rejected 先中）"):
    writeConfig("""{"ir":{"policy":{"rulez":[]}}}""")
    val reduced = new PolicyEngine(PolicyConfig(knownCapKinds = Set.empty))
    IrPolicyRules.view(reduced).map { view =>
      val t = targetOf("dev:test:probe", Set(Cap.Exec))
      val d = view.decide(Tenant.Human("u"), Ingress.Human, t, Safety.AutoAll)
      // 无 denyAll 时（词表不含 Exec）应为 fail-closed:unknown-cap；被拒配置 ⇒ config-rejected 先中
      assertEquals(d.asInstanceOf[Decision.Deny].rule, "policy:config-rejected")
    }

  // ── ④ 重复匹配键 vs 不同键重叠 ────────────────────────────────────────

  test("④ 重复匹配键 ⇒ Left 列两条下标；不同键重叠(精确+通配)合法且各自命中"):
    writeConfig(
      policyCfg(
        """{"name":"a:b","decision":"allow"}""" + "," +
          """{"name":"a:b","decision":"deny","reason":"r","rule":"r"}"""
      )
    )
    IrPolicyRules.load.flatMap {
      case Left(msg) =>
        assertEquals(msg.contains("rules[0]") && msg.contains("rules[1]"), true)
        // 不同键重叠：精确 Allow + 通配 Deny = 特例压通例的分层用法，合法
        writeConfig(
          policyCfg(
            """{"name":"dev:test:probe","decision":"allow"}""" + "," +
              """{"name":"dev:test:*","decision":"deny","reason":"wild","rule":"cfg:wild"}"""
          )
        )
        IrTestKit.vfs().flatMap { root =>
          IrTestKit.harness(root).flatMap { h =>
            for
              view <- IrPolicyRules.view(h.policy)
              rProbe <- submit(h, IrTestKit.call("dev:test:probe"), Safety.ConfirmEdits, Some(view))
              rSink <- submit(h, IrTestKit.call("dev:test:sink"), Safety.ConfirmEdits, Some(view))
            yield
              assertEquals(rProbe.status, Status.Done) // 精确 Allow 先中
              assertEquals(rSink.status, Status.Rejected) // 通配 Deny 罩其余
              assertEquals(rSink.error.flatMap(_.details("rule").flatMap(_.asString)), Some("cfg:wild"))
            end for
          }
        }
      case Right(rules) => fail(s"duplicate matching key must be rejected, got $rules")
    }

  // ── ⑤ 缺省三态：无配置 ⇒ 与无 override 的 submit 逐字段相等 ─────────────

  test("⑤ 缺 ir/ir=null/policy=null/空数组 ⇒ Right(Nil)，判定=无 override 的 submit"):
    val shapes = List(
      "缺 ir 节" -> """{"providers":{}}""",
      "ir 为 null" -> """{"ir":null}""",
      "policy 为 null" -> """{"ir":{"policy":null}}""",
      "llmIngress 并存无 policy" -> """{"ir":{"llmIngress":true}}""",
      "空数组" -> """{"ir":{"policy":{"rules":[]}}}"""
    )
    shapes.traverse_ { case (label, cfg) =>
      writeConfig(cfg)
      IrPolicyRules.load.flatMap { loaded =>
        assertEquals(loaded, Right(Nil), clue = label)
        IrTestKit.vfs().flatMap { root =>
          IrTestKit.harness(root).flatMap { h =>
            // probe 带 FsWrite cap ⇒ 无规则 ⇒ default:no-rule Ask ⇒ await_approval
            // （零执行 ⇒ 无 durationMs 噪声，PlanResult 可整体相等）
            IrPolicyRules.view(h.policy).flatMap { v =>
              for
                withView <- submit(h, IrTestKit.call("dev:test:probe"), Safety.ConfirmEdits, Some(v))
                without <- submit(h, IrTestKit.call("dev:test:probe"), Safety.ConfirmEdits, None)
              yield assertEquals(withView, without)
              end for
            }
          }
        }
      }
    }

  test("⑤b 缺文件（beforeEach 只建空 tmp 目录）⇒ Right(Nil)"):
    IrPolicyRules.load.map(loaded => assertEquals(loaded, Right(Nil)))

  // ── ⑥ tenant/capKinds 维度（decide 直测，不受测试命令 audience 面干扰）──

  test("⑥ tenant 过滤：tenant=llm 规则不匹配 human 租户（落基线通配）、匹配 llm 租户"):
    writeConfig(policyCfg("""{"name":"dev:test:probe","decision":"allow","tenant":"llm"}"""))
    IrPolicyRules.view(baseEngine).map { view =>
      val t = targetOf("dev:test:probe", Set(Cap.FsWrite(".")))
      val human = view.decide(Tenant.Human("u"), Ingress.Human, t, Safety.ConfirmEdits)
      val llm = view.decide(Tenant.Llm("s", "a"), Ingress.Llm, t, Safety.ConfirmEdits)
      assertEquals(human.isInstanceOf[Decision.Ask], true) // 用户规则不匹配 ⇒ 基线通配 Ask
      assertEquals(human.asInstanceOf[Decision.Ask].rule, "base:ask")
      assertEquals(llm, Decision.Allow) // 用户规则匹配
    }

  test("⑥b capKinds 过滤：[Exec] 不匹配四件套 ext 命令、匹配纯 Exec 命令"):
    writeConfig(policyCfg("""{"name":"ext:tool:x","decision":"allow","capKinds":["Exec"]}"""))
    IrPolicyRules.view(baseEngine).map { view =>
      val four = targetOf("ext:tool:x", Set(Cap.Exec, Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Net("*")))
      val execOnly = targetOf("ext:tool:x", Set(Cap.Exec))
      val fourD = view.decide(Tenant.Human("u"), Ingress.Human, four, Safety.ConfirmEdits)
      val execD = view.decide(Tenant.Human("u"), Ingress.Human, execOnly, Safety.ConfirmEdits)
      assertEquals(fourD.isInstanceOf[Decision.Ask], true) // caps ⊄ {Exec} ⇒ 规则不匹配 ⇒ default:no-rule
      assertEquals(fourD.asInstanceOf[Decision.Ask].rule, "default:no-rule")
      assertEquals(execD, Decision.Allow) // caps ⊆ {Exec} ⇒ 匹配
    }

  test("⑥c 合法 capKinds 全词表 + tenant=external 可解"):
    writeConfig(
      policyCfg(
        """{"name":"a:b","decision":"deny","reason":"r","rule":"r","capKinds":["Exec","FsRead","FsWrite","Net","Secret","MemoryWrite"],"tenant":"external"}"""
      )
    )
    IrPolicyRules.load.map(loaded => assertEquals(loaded.isRight, true))

  // ── ⑦ ir 节点级：未知兄弟键容忍（WARN 留痕不拒）────────────────────────

  test("⑦ ir 节点级容忍：未知兄弟键不拒；llmIngress 与 policy 并存互不扰"):
    writeConfig(
      """{"ir":{"policies":{"rules":[]},"llmIngress":true,"policy":{"rules":[{"name":"dev:test:probe","decision":"allow"}]}}}"""
    )
    IrPolicyRules.load.flatMap { loaded =>
      assertEquals(loaded.map(_.length), Right(1)) // 兄弟键 policies 不拒
      IrTestKit.vfs().flatMap { root =>
        IrTestKit.harness(root).flatMap { h =>
          IrPolicyRules.view(h.policy).flatMap { v =>
            submit(h, IrTestKit.call("dev:test:probe"), Safety.ConfirmEdits, Some(v)).map { r =>
              assertEquals(r.status, Status.Done) // policy 规则照常生效
            }
          }
        }
      }
    }

  test("⑦b ir 节非对象 ⇒ 容忍为无规则（宽读现状，WARN 留痕）"):
    writeConfig("""{"ir":"oops"}""")
    IrPolicyRules.load.map(loaded => assertEquals(loaded, Right(Nil)))

  // ── ⑧ forRequest 纯函数 ───────────────────────────────────────────────

  test("⑧ forRequest 纯函数：同参两次派生判定一致"):
    val t = targetOf("dev:test:probe", Set(Cap.FsWrite(".")))
    val a = baseEngine.forRequest(List(PolicyRule("dev:test:probe", Decision.Allow)))
    val b = baseEngine.forRequest(List(PolicyRule("dev:test:probe", Decision.Allow)))
    assertEquals(
      a.decide(Tenant.Human("u"), Ingress.Human, t, Safety.ConfirmEdits),
      b.decide(Tenant.Human("u"), Ingress.Human, t, Safety.ConfirmEdits)
    )

end IrPolicyRulesSpec
