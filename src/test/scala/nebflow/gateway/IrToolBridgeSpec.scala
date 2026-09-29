package nebflow.gateway

import cats.effect.IO
import io.circe.syntax.*
import io.circe.JsonObject
import munit.CatsEffectSuite
import nebflow.core.tools.{RemoteExecutor, ToolRegistry}
import nebflow.ir.*

/**
 * P1-2 批 A 桥接的一致性用例：声明级表驱动（七件全覆盖 / 双向完备 / 单键空间不越界 /
 * 逐件精确 caps 集 / 机械 params 纪律 / 注册与幂等 reindex）+ 三条真实经 IR 执行
 * （Read 真文件 / NodeCancel 确定性失败 / policy Ask@ConfirmEdits）+ LLM 可见面钉
 * （§7.4 检查点①：桥不开模型面侧门）。
 *
 * caps 断言是**精确集合相等**（既是下限也是上限）：glob/grep 退回仅 FsRead（漏报
 * Exec）必红；给 glob 塞第三枚 cap 也红。
 */
class IrToolBridgeSpec extends CatsEffectSuite:

  private val defs: List[CommandDef] = IrToolBridge.defs(None)

  private def byName(name: String): CommandDef =
    defs.find(_.name == name).getOrElse(fail(s"missing bridged def $name"))

  private def schemaProps(cmd: CommandDef): JsonObject =
    cmd.argsSchema("properties").flatMap(_.asObject).getOrElse(JsonObject.empty)

  private def propType(cmd: CommandDef, key: String): Option[String] =
    schemaProps(cmd)(key).flatMap(_.asObject).flatMap(_("type")).flatMap(_.asString)

  private def run(
    h: IrTestKit.Harness,
    plan: Ir,
    safety: Safety = Safety.AutoAll,
    tenant: Tenant = Tenant.Human("local"),
    ingress: Ingress = Ingress.Human,
    approval: Option[ApprovalCredential] = None
  ): IO[PlanResult] =
    h.router.submit(IrTestKit.req(plan, tenant, ingress, approval), VfsRoot(h.root), safety)

  /** 带桥接 defs + askRule 的 harness（真实工具执行腿共用）。 */
  private def bridgedHarness(root: os.Path): IO[IrTestKit.Harness] =
    IrTestKit.harness(root, rules = IrToolCaps.askRules, extra = defs)

  // ── 声明级：七件全覆盖 / 命名与 binding ──────────────────────

  test("表驱动·七件全覆盖：恰 7 件，irName 集合精确相等，语法/首段/binding 全过"):
    assertEquals(defs.length, 7)
    assertEquals(
      defs.map(_.name).toSet,
      Set(
        "dev:tool:read",
        "dev:tool:glob",
        "dev:tool:grep",
        "dev:tool:nodelist",
        "dev:tool:nodecancel",
        "dev:tool:node_report",
        "dev:tool:listfriends"
      )
    )
    defs.foreach { cmd =>
      assertEquals(Names.syntax(cmd.name), Right(()), s"syntax of ${cmd.name}")
      assertEquals(cmd.name.split(":", -1).head, "dev", s"namespace of ${cmd.name}")
      assert(cmd.binding.isInstanceOf[Binding.Dev], s"binding of ${cmd.name}")
      assert(cmd.description.nonEmpty, s"description of ${cmd.name}")
    }

  test("表驱动·双向完备：toolName ∈ TOOL_MAP 且实例非空，irName↔toolName 双射，dev:fs 不被掺入"):
    val rows = IrToolCaps.bridged
    rows.foreach { row =>
      val tool = ToolRegistry.TOOL_MAP.getOrElse(row.toolName, null)
      assert(tool != null, s"TOOL_MAP must contain ${row.toolName}")
      assert(tool.name == row.toolName, s"registry instance name drift for ${row.toolName}")
    }
    assertEquals(rows.map(_.irName).distinct.length, rows.length)
    assertEquals(rows.map(_.toolName).distinct.length, rows.length)
    assertEquals(defs.map(d => d.name -> d.llmName).toMap, rows.map(r => r.irName -> Some(r.toolName)).toMap)
    // 反向完备：桥接 defs 不含 dev:fs 两件；两件也不在桥表里
    assert(defs.map(_.name).forall(!_.startsWith("dev:fs:")))
    assert(rows.map(_.irName).forall(!_.startsWith("dev:fs:")))

  test("表驱动·argsSchema 机械生成：逐字节 == augmentSchema 后的 inputSchema（保持 device 面）"):
    IrToolCaps.bridged.foreach { row =>
      val tool = ToolRegistry.TOOL_MAP(row.toolName)
      val cmd = byName(row.irName)
      assertEquals(
        cmd.argsSchema,
        RemoteExecutor.augmentSchema(tool.name, tool.inputSchema),
        s"schema of ${row.irName}"
      )
    }
    // remoteable 六件中的本批三件带 device；其余四件不带
    assertEquals(propType(byName("dev:tool:read"), "device"), Some("string"))
    assertEquals(propType(byName("dev:tool:glob"), "device"), Some("string"))
    assertEquals(propType(byName("dev:tool:grep"), "device"), Some("string"))
    List("dev:tool:nodelist", "dev:tool:nodecancel", "dev:tool:node_report", "dev:tool:listfriends").foreach { n =>
      assertEquals(schemaProps(byName(n)).contains("device"), false, s"device must not be injected into $n")
    }

  // ── 声明级：逐件精确 caps 集（评审冻结：glob/grep 必含 Exec）────────────

  test("表驱动·逐件精确 caps 集（集合相等 = 下限也是上限）+ cap kind 全 ∈ knownCapKinds"):
    val expected: Map[String, Set[Cap]] = Map(
      "dev:tool:read" -> Set(Cap.FsRead("*")),
      "dev:tool:glob" -> Set(Cap.FsRead("*"), Cap.Exec),
      "dev:tool:grep" -> Set(Cap.FsRead("*"), Cap.Exec),
      "dev:tool:nodelist" -> Set(Cap.FsRead("*")),
      "dev:tool:nodecancel" -> Set(Cap.FsWrite("*")),
      "dev:tool:node_report" -> Set(Cap.FsWrite("*")),
      "dev:tool:listfriends" -> Set(Cap.Net("*"))
    )
    defs.foreach { cmd =>
      assertEquals(cmd.caps, expected(cmd.name), s"caps of ${cmd.name}")
    }
    val known = PolicyConfig().knownCapKinds
    defs.foreach { cmd =>
      assert(cmd.capKinds.forall(known.contains), s"cap kinds of ${cmd.name} must be known to the engine")
    }

  test("表驱动·pathArgs/io/audiences/llmName：七件统一（pathArgs=∅、io=Text、Human、llmName=旧名）"):
    defs.foreach { cmd =>
      assertEquals(cmd.pathArgs, Set.empty[String], s"pathArgs of ${cmd.name}")
      assertEquals(cmd.io, CommandIo(stdin = None, stdout = StreamKind.Text), s"io of ${cmd.name}")
      assertEquals(cmd.audiences, Set(Audience.Human), s"audiences of ${cmd.name}")
      assertEquals(
        cmd.llmName,
        IrToolCaps.bridged.find(_.irName == cmd.name).map(_.toolName),
        s"llmName of ${cmd.name}"
      )
      assertEquals(cmd.trust, Trust.Builtin, s"trust of ${cmd.name}")
      assertEquals(cmd.delivery, Delivery.Both, s"delivery of ${cmd.name}")
    }

  // ── 声明级：params 机械纪律（全 named；字符串面才进糖）──────────────────

  test("表驱动·params 纪律：全 named 无 positional、名 ∈ properties、number/boolean/dash 属性不进糖"):
    defs.foreach { cmd =>
      val props = schemaProps(cmd)
      val paramNames = cmd.params.map(_.name)
      assertEquals(paramNames.distinct.length, paramNames.length, s"distinct params of ${cmd.name}")
      cmd.params.foreach { p =>
        assertEquals(p.positional, None, s"${cmd.name} param ${p.name} must be named-only")
        assert(props.contains(p.name), s"${cmd.name} param ${p.name} must be a schema property")
        assert(p.name.matches("^[A-Za-z][A-Za-z0-9_-]*$"), s"${cmd.name} param ${p.name} must be a legal flag key")
      }
      // 机械规则镜像：type ∈ {number,integer,boolean} 的属性一律不进糖（糖值恒字符串）
      val nonSugar = props.keys.filter { k =>
        propType(cmd, k).exists(t => t == "number" || t == "integer" || t == "boolean")
      }.toSet
      assertEquals(paramNames.toSet.intersect(nonSugar), Set.empty[String], s"non-string props of ${cmd.name}")
      // required 从 schema 机械镜像
      val required = cmd.argsSchema("required").flatMap(_.asArray).map(_.flatMap(_.asString).toSet).getOrElse(Set.empty)
      assertEquals(
        cmd.params.filter(_.required).map(_.name).toSet,
        required.intersect(paramNames.toSet),
        s"required mirror of ${cmd.name}"
      )
    }
    // 冻结锚：Grep 的 -i/-A/-B/-C 与 head_limit/offset 不在糖面；Read 的 offset/limit 同理
    val grep = byName("dev:tool:grep").params.map(_.name).toSet
    assertEquals(grep.intersect(Set("-i", "-A", "-B", "-C", "head_limit", "offset", "multiline")), Set.empty[String])
    val read = byName("dev:tool:read").params.map(_.name).toSet
    assertEquals(read.intersect(Set("offset", "limit")), Set.empty[String])
    // 零参命令（ListFriends）糖面为空
    assertEquals(byName("dev:tool:listfriends").params, Nil)
    // NodeList.status（oneOf 含 string 分支）机械进糖
    assert(byName("dev:tool:nodelist").params.map(_.name).contains("status"))

  // ── 注册与幂等 reindex ──────────────────────────────────────

  test("注册与幂等：七件注册全 Right；同档重注册=N4 碰撞；reindex 连跑两次不丢件"):
    IO.delay {
      val reg = new CommandRegistry()
      DevCommands.base.foreach(c => reg.register(c).fold(e => fail(s"base ${c.name}: ${e.message}"), _ => ()))
      defs.foreach(cmd => reg.register(cmd).fold(e => fail(s"register ${cmd.name}: ${e.message}"), _ => ()))
      // 同档（Builtin）重注册同一 IR 名 = N4 碰撞拒绝 ⇒ reindex 必须先摘后挂
      val dup = reg.register(defs.head)
      assert(dup.isLeft, "same-tier re-registration must be an N4 collision")
      // 先摘后挂 ⇒ Right
      reg.unregister(defs.head.name)
      assertEquals(reg.register(defs.head).map(_.name), Right(defs.head.name))
      // 幂等 reindex ×2：七件仍在，名集恰为 fs 两件 + tool 七件
      IrToolBridge.reindex(reg, None)
      IrToolBridge.reindex(reg, None)
      assertEquals(reg.names.toSet, (DevCommands.base.map(_.name) ++ defs.map(_.name)).toSet)
      assert(reg.get("dev:tool:read").isDefined)
      assert(reg.get("dev:tool:listfriends").isDefined)
    }

  test("单键空间不越界：ToolRegistry 无任何 dev: 前缀键（桥只读 TOOL_MAP）"):
    IO.delay {
      assert(ToolRegistry.registeredToolNames.forall(!_.startsWith("dev:")))
      assert(ToolRegistry.builtinToolNames.forall(!_.startsWith("dev:")))
    }

  // ── 真实执行（经 Router/Planner/Executor 全链，非 stub）─────────────────

  test("真实执行①：dev:tool:read 读真文件（AutoAll 折 Ask→Allow）⇒ done/exit=0/final 以 1\\t 行号形态"):
    IO.blocking(os.temp(contents = "hello-bridge-line")).flatMap { file =>
      IrTestKit.vfs().flatMap { root =>
        bridgedHarness(root).flatMap { h =>
          val plan = Ir.Call("dev:tool:read", JsonObject("file_path" -> file.toString.asJson))
          run(h, plan).map { r =>
            assertEquals(r.status, Status.Done, s"error=${r.error.map(_.message)}")
            assertEquals(r.exit, Some(0))
            assertEquals(r.results.head.command, "dev:tool:read")
            r.finalStdout match
              case Some(StreamValue.Text(t)) => assert(t.startsWith("1\t"), s"cat -n form expected, got: $t")
              case other => fail(s"expected text stdout, got $other")
          }
        }
      }
    }

  test(
    "真实执行②：dev:tool:nodecancel（缺 project）⇒ done/exit=1/command.failed + Missing 'project'（Left→command.failed 映射实证）"
  ):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        val plan = Ir.Call("dev:tool:nodecancel", JsonObject("node-id" -> "smoke-node-1".asJson))
        run(h, plan).map { r =>
          assertEquals(r.status, Status.Done)
          assertEquals(r.exit, Some(1))
          assertEquals(r.results.head.error.map(_.code), Some(Codes.CommandFailed))
          assert(
            r.results.head.error.exists(_.message.contains("Missing 'project' parameter")),
            s"message=${r.results.head.error.map(_.message)}"
          )
        }
      }
    }

  test("真实执行②b：dev:tool:node_report（桥接 ctx 无节点身份）⇒ done/exit=1/NODE_REPORT_FORBIDDEN（确定性负测，诚实失败）"):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        val plan = Ir.Call(
          "dev:tool:node_report",
          JsonObject("category" -> "blocked".asJson, "detail" -> "smoke-detail".asJson)
        )
        run(h, plan).map { r =>
          assertEquals(r.status, Status.Done)
          assertEquals(r.exit, Some(1))
          assertEquals(r.results.head.error.map(_.code), Some(Codes.CommandFailed))
          assert(
            r.results.head.error.exists(_.message.contains("NODE_REPORT_FORBIDDEN")),
            s"message=${r.results.head.error.map(_.message)}"
          )
        }
      }
    }

  test("真实执行③·policy Ask：ConfirmEdits 下 dev:tool:read ⇒ await_approval/零执行/审批体含 caps 与规则名；审计恰一条 ask"):
    IO.blocking(os.temp(contents = "ask-face")).flatMap { file =>
      IrTestKit.vfs().flatMap { root =>
        bridgedHarness(root).flatMap { h =>
          val plan = Ir.Call("dev:tool:read", JsonObject("file_path" -> file.toString.asJson))
          run(h, plan, safety = Safety.ConfirmEdits).flatMap { r =>
            h.records.flatMap { records =>
              IO.delay {
                assertEquals(r.status, Status.AwaitApproval)
                assertEquals(r.exit, None)
                assertEquals(r.results, Nil)
                val ap = r.approval.getOrElse(fail("approval body missing"))
                assertEquals(ap.nodes.map(_.command), List("dev:tool:read"))
                assertEquals(ap.nodes.head.caps, List("FsRead(*)"))
                assertEquals(ap.rule, "bridge:opaque-host")
                // §8.6：恰一条、decision=ask、capsSource=declared、argsDigest 在而 args 原文不在
                assertEquals(records.length, 1)
                val rec = records.head
                assertEquals(rec.decision, "ask")
                assertEquals(rec.capsSource, Codes.CapsDeclared)
                assertEquals(rec.caps, List("FsRead(*)"))
                assert(rec.argsDigest.nonEmpty)
                assert(!rec.toJson.asObject.get.contains("args"))
              }
            }
          }
        }
      }
    }

  test("LLM 可见面钉：llm ingress 直连 ir 帧调 dev:tool:read ⇒ invalid + router.invalid_args(reason=audience)（桥未开 LLM 侧门）"):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        val plan = Ir.Call("dev:tool:read", JsonObject("file_path" -> "/etc/hosts".asJson))
        run(h, plan, tenant = Tenant.Llm("sess-1", "agent-a"), ingress = Ingress.Llm).map { r =>
          assertEquals(r.status, Status.Invalid)
          assertEquals(r.exit, Some(2))
          assertEquals(r.error.map(_.code), Some(Codes.InvalidArgs))
          assertEquals(r.error.flatMap(_.details("reason")).flatMap(_.asString), Some("audience"))
        }
      }
    }

end IrToolBridgeSpec
