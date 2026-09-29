package nebflow.gateway

import cats.effect.IO
import io.circe.syntax.*
import io.circe.{Json, JsonObject}
import munit.CatsEffectSuite
import nebflow.core.tools.{RemoteExecutor, ToolRegistry}
import nebflow.ir.*
import nebflow.shared.PathUtil

/**
 * P1-2 批 A + 批 B 桥接的一致性用例：声明级表驱动（十五件全覆盖 / 双向完备 /
 * 单键空间不越界 / 逐件精确 caps 集 / 机械 params 纪律 / 注册与幂等 reindex）+
 * 真实经 IR 执行（Read 真文件 / **写面正负两腿** / tasklist 隔离根 / 确定性负测四条 /
 * policy Ask@ConfirmEdits）+ LLM 可见面钉（§7.4 检查点①：桥不开模型面侧门）。
 *
 * 批 B 写面核心验证点（缺一不可）：
 *  - 正腿：显式 Allow 规则（`dev:tool:write` 精确名）+ Safety.ConfirmEdits ⇒ 真写
 *    成功——证明放行来自规则而非 auto-all 折叠；
 *  - 负腿：harness rules=Nil（无 askRule）+ Safety.ConfirmEdits ⇒ `await_approval`
 *    且目标文件零变化——无规则时 FsWrite 走 `default:no-rule` 缺省 Ask（Policy 缺省
 *    分支），执行前短路 ⇒ 零副作用。
 *
 * caps 断言是**精确集合相等**（既是下限也是上限）：glob/grep 退回仅 FsRead（漏报
 * Exec）必红；nodeedit 漏报 Exec 同红；给任一件塞多余 cap 也红。
 *
 * 套件级 `PathUtil.setDataRoot` 隔离（TaskListToolSpec 同款配方）：TaskList 真实写
 * 腿的存储根是全局 `dataRoot`（非 VFS root），不换根会污染真实宿主 tasks.json。
 */
class IrToolBridgeSpec extends CatsEffectSuite:

  private val defs: List[CommandDef] = IrToolBridge.defs(None)

  private var dataRootHome: os.Path = null
  private var prevRoot: os.Path = null

  override def beforeAll(): Unit =
    super.beforeAll()
    prevRoot = PathUtil.dataRoot
    dataRootHome = os.temp.dir(prefix = "nebflow-ir-batchb-")
    PathUtil.setDataRoot(dataRootHome)

  override def afterAll(): Unit =
    PathUtil.setDataRoot(prevRoot)
    os.remove.all(dataRootHome)
    super.afterAll()

  private def byName(name: String): CommandDef =
    defs.find(_.name == name).getOrElse(fail(s"missing bridged def $name"))

  private def schemaProps(cmd: CommandDef): JsonObject =
    cmd.argsSchema("properties").flatMap(_.asObject).getOrElse(JsonObject.empty)

  private def propType(cmd: CommandDef, key: String): Option[String] =
    schemaProps(cmd)(key).flatMap(_.asObject).flatMap(_("type")).flatMap(_.asString)

  /** [[IrToolBridge.admitsString]] 的测试侧镜像（string 或 oneOf 含 string 分支）。 */
  private def admitsStringMirror(prop: Json): Boolean =
    val obj = prop.asObject.getOrElse(JsonObject.empty)
    obj("type").flatMap(_.asString) match
      case Some("string") => true
      case Some(_) => false
      case None =>
        obj("oneOf")
          .flatMap(_.asArray)
          .exists(_.exists(b => b.asObject.exists(o => o("type").flatMap(_.asString).contains("string"))))

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

  private def walk(p: os.Path): Set[String] = os.walk(p).map(_.toString).toSet

  // ── 声明级：十五件全覆盖 / 命名与 binding ──────────────────────

  test("表驱动·十五件全覆盖：恰 15 件，irName 集合精确相等，语法/首段/binding 全过"):
    assertEquals(defs.length, 15)
    assertEquals(
      defs.map(_.name).toSet,
      Set(
        // 批 A 只读七件
        "dev:tool:read",
        "dev:tool:glob",
        "dev:tool:grep",
        "dev:tool:nodelist",
        "dev:tool:nodecancel",
        "dev:tool:node_report",
        "dev:tool:listfriends",
        // 批 B 写面八件（irName = dev:tool:<lower(旧名)>，机械命名规则）
        "dev:tool:write",
        "dev:tool:edit",
        "dev:tool:pop",
        "dev:tool:card",
        "dev:tool:projectcreate",
        "dev:tool:nodeedit",
        "dev:tool:tasklist",
        "dev:tool:taskboard"
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
    // remoteable 六件中的本批五件带 device（remoteableTools = Bash/Read/Write/Edit/Glob/Grep）；
    // 其余十件不带
    List("dev:tool:read", "dev:tool:write", "dev:tool:edit", "dev:tool:glob", "dev:tool:grep").foreach { n =>
      assertEquals(propType(byName(n), "device"), Some("string"), s"device of $n")
    }
    List(
      "dev:tool:nodelist",
      "dev:tool:nodecancel",
      "dev:tool:node_report",
      "dev:tool:listfriends",
      "dev:tool:pop",
      "dev:tool:card",
      "dev:tool:projectcreate",
      "dev:tool:nodeedit",
      "dev:tool:tasklist",
      "dev:tool:taskboard"
    ).foreach { n =>
      assertEquals(schemaProps(byName(n)).contains("device"), false, s"device must not be injected into $n")
    }

  // ── 声明级：逐件精确 caps 集（评审冻结：glob/grep/nodeedit 必含 Exec）────────

  test("表驱动·逐件精确 caps 集（集合相等 = 下限也是上限）+ cap kind 全 ∈ knownCapKinds"):
    val expected: Map[String, Set[Cap]] = Map(
      // 批 A 七件（原值不动）
      "dev:tool:read" -> Set(Cap.FsRead("*")),
      "dev:tool:glob" -> Set(Cap.FsRead("*"), Cap.Exec),
      "dev:tool:grep" -> Set(Cap.FsRead("*"), Cap.Exec),
      "dev:tool:nodelist" -> Set(Cap.FsRead("*")),
      "dev:tool:nodecancel" -> Set(Cap.FsWrite("*")),
      "dev:tool:node_report" -> Set(Cap.FsWrite("*")),
      "dev:tool:listfriends" -> Set(Cap.Net("*")),
      // 批 B 八件（写面 prefix="*" 诚实形态；Pop/Card 按读通道声明）
      "dev:tool:write" -> Set(Cap.FsWrite("*")),
      "dev:tool:edit" -> Set(Cap.FsWrite("*")),
      "dev:tool:pop" -> Set(Cap.FsRead("*")),
      "dev:tool:card" -> Set(Cap.FsRead("*")),
      "dev:tool:projectcreate" -> Set(Cap.FsWrite("*")),
      "dev:tool:nodeedit" -> Set(Cap.FsWrite("*"), Cap.Exec),
      "dev:tool:tasklist" -> Set(Cap.FsWrite("*")),
      "dev:tool:taskboard" -> Set(Cap.FsWrite("*"))
    )
    defs.foreach { cmd =>
      assertEquals(cmd.caps, expected(cmd.name), s"caps of ${cmd.name}")
    }
    val known = PolicyConfig().knownCapKinds
    defs.foreach { cmd =>
      assert(cmd.capKinds.forall(known.contains), s"cap kinds of ${cmd.name} must be known to the engine")
    }

  test("表驱动·pathArgs/io/audiences/llmName：十五件统一（pathArgs=∅、io=Text、Human、llmName=旧名）"):
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

  test("表驱动·params 纪律：全 named 无 positional、名 ∈ properties、糖面恰等于 admitsString 镜像"):
    defs.foreach { cmd =>
      val props = schemaProps(cmd)
      val paramNames = cmd.params.map(_.name)
      assertEquals(paramNames.distinct.length, paramNames.length, s"distinct params of ${cmd.name}")
      cmd.params.foreach { p =>
        assertEquals(p.positional, None, s"${cmd.name} param ${p.name} must be named-only")
        assert(props.contains(p.name), s"${cmd.name} param ${p.name} must be a schema property")
        assert(p.name.matches("^[A-Za-z][A-Za-z0-9_-]*$"), s"${cmd.name} param ${p.name} must be a legal flag key")
        assertEquals(p.default, None, s"${cmd.name} param ${p.name} carries no default ([D19])")
      }
      // 机械规则全镜像（糖面 =「合法 flag 键 ∧ admitsString」的属性集，不多不少）
      val mirror = props.keys
        .filter(k => k.matches("^[A-Za-z][A-Za-z0-9_-]*$") && admitsStringMirror(props(k).getOrElse(Json.Null)))
        .toSet
      assertEquals(paramNames.toSet, mirror, s"sugar face of ${cmd.name} must equal the admitsString mirror")
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
    // 批 B 锚①：Edit 的 replace_all（boolean）不进糖；Write/Edit 的 content/file_path/
    // old_string/new_string（string）进；两件均 remoteable ⇒ augmentSchema 注入的
    // device（string）机械进糖（与 device 面断言互证）
    val edit = byName("dev:tool:edit").params.map(_.name).toSet
    assertEquals(edit, Set("file_path", "old_string", "new_string", "device"))
    val write = byName("dev:tool:write").params.map(_.name).toSet
    assertEquals(write, Set("file_path", "content", "device"))
    // 批 B 锚②：NodeEdit 的 retry（oneOf 含 string 分支，"<id>:<N>" 串形态）进糖，
    // maxRounds（integer）/worktree（boolean）不进
    val nodeEdit = byName("dev:tool:nodeedit").params.map(_.name).toSet
    assert(nodeEdit.contains("retry"))
    assertEquals(nodeEdit.intersect(Set("maxRounds", "worktree")), Set.empty[String])
    // 批 B 锚③：TaskList/TaskBoard 的 blocks/links（array）不进；action/title（string）进
    val taskList = byName("dev:tool:tasklist").params.map(_.name).toSet
    assertEquals(taskList.intersect(Set("blocks", "links")), Set.empty[String])
    assert(taskList.contains("action") && taskList.contains("title"))
    val taskBoard = byName("dev:tool:taskboard").params.map(_.name).toSet
    assertEquals(taskBoard.intersect(Set("blocks", "links")), Set.empty[String])
    assert(taskBoard.contains("action") && taskBoard.contains("title"))

  // ── 注册与幂等 reindex ──────────────────────────────────────

  test("注册与幂等：十五件注册全 Right；同档重注册=N4 碰撞；reindex 连跑两次不丢件"):
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
      // 幂等 reindex ×2：十五件仍在，名集恰为 fs 两件 + tool 十五件
      IrToolBridge.reindex(reg, None)
      IrToolBridge.reindex(reg, None)
      assertEquals(reg.names.toSet, (DevCommands.base.map(_.name) ++ defs.map(_.name)).toSet)
      assert(reg.get("dev:tool:read").isDefined)
      assert(reg.get("dev:tool:write").isDefined)
      assert(reg.get("dev:tool:taskboard").isDefined)
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

  // ── 真实执行·批 B 写面核心验证点（正负两腿缺一不可）────────────────────

  test("真实执行·写面正腿：显式 Allow 规则 + ConfirmEdits ⇒ dev:tool:write 真写临时文件成功（规则放行，非 auto-all 折叠）"):
    IO.blocking(os.temp.dir(prefix = "nebflow-ir-write-allow-")).flatMap { dir =>
      IrTestKit.vfs().flatMap { root =>
        // 精确名 Allow 规则排在通配 askRule 前（rules.find 首个匹配生效）⇒ 证明是
        // 规则放行；ConfirmEdits 下若无规则必 await_approval（负腿钉住）
        IrTestKit
          .harness(
            root,
            rules = PolicyRule(name = "dev:tool:write", decision = Decision.Allow) :: IrToolCaps.askRules,
            extra = defs
          )
          .flatMap { h =>
            val target = dir / "created-by-bridge.txt"
            val plan = Ir.Call(
              "dev:tool:write",
              JsonObject("file_path" -> target.toString.asJson, "content" -> "batch-b write leg".asJson)
            )
            run(h, plan, safety = Safety.ConfirmEdits).flatMap { r =>
              h.records.map { records =>
                assertEquals(r.status, Status.Done, s"error=${r.error.map(_.message)}")
                assertEquals(r.exit, Some(0))
                r.finalStdout match
                  case Some(StreamValue.Text(t)) =>
                    assert(t.startsWith("OK:CREATED"), s"OK:CREATED expected, got: $t")
                  case other => fail(s"expected text stdout, got $other")
                // 写-读对账：真实落盘
                assertEquals(os.read(target), "batch-b write leg")
                // 审计恰一条 allow（决策来自规则而非档位）
                assertEquals(records.length, 1)
                assertEquals(records.head.command, "dev:tool:write")
                assertEquals(records.head.decision, "allow")
                assertEquals(records.head.caps, List("FsWrite(*)"))
                assertEquals(records.head.rule, None)
              }
            }
          }
      }
    }

  test("真实执行·写面负腿：无规则 + ConfirmEdits ⇒ FsWrite 缺省 Ask（default:no-rule）、await_approval 且目标文件零变化"):
    IO.blocking(os.temp.dir(prefix = "nebflow-ir-write-ask-")).flatMap { dir =>
      IrTestKit.vfs().flatMap { root =>
        // rules=Nil：连桥的 dev:tool:* askRule 也不装 ⇒ 走 Policy 缺省分支
        // （FsWrite ⊄ {FsRead} ⇒ Ask("default:no-rule")）——引擎事实的负测锚
        IrTestKit.harness(root, rules = Nil, extra = defs).flatMap { h =>
          val target = dir / "must-not-exist.txt"
          val plan = Ir.Call(
            "dev:tool:write",
            JsonObject("file_path" -> target.toString.asJson, "content" -> "must never land".asJson)
          )
          run(h, plan, safety = Safety.ConfirmEdits).flatMap { r =>
            h.records.map { records =>
              assertEquals(r.status, Status.AwaitApproval)
              assertEquals(r.exit, None)
              assertEquals(r.results, Nil)
              val ap = r.approval.getOrElse(fail("approval body missing"))
              assertEquals(ap.nodes.map(_.command), List("dev:tool:write"))
              assertEquals(ap.nodes.head.caps, List("FsWrite(*)"))
              // 零副作用：目标文件不存在（执行前短路）
              assertEquals(os.exists(target), false, "no side effect before approval")
              // §8.6：恰一条、decision=ask、rule=default:no-rule、capsSource=declared、
              // argsDigest 在而 args 原文不在
              assertEquals(records.length, 1)
              val rec = records.head
              assertEquals(rec.decision, "ask")
              assertEquals(rec.rule, Some("default:no-rule"))
              assertEquals(rec.capsSource, Codes.CapsDeclared)
              assertEquals(rec.caps, List("FsWrite(*)"))
              assert(rec.argsDigest.nonEmpty)
              assert(!rec.toJson.asObject.get.contains("args"))
            }
          }
        }
      }
    }

  test("真实执行·dev:tool:tasklist：隔离根 create + list（真实 TaskListTool 写读经 IR 全链，零污染真实 dataRoot）"):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        val create = Ir.Call(
          "dev:tool:tasklist",
          JsonObject("action" -> "create".asJson, "title" -> "batch-b bridge entry".asJson)
        )
        run(h, create).flatMap { r1 =>
          val list = Ir.Call("dev:tool:tasklist", JsonObject("action" -> "list".asJson))
          run(h, list).map { r2 =>
            assertEquals(r1.status, Status.Done, s"create error=${r1.error.map(_.message)}")
            assertEquals(r1.exit, Some(0))
            r1.finalStdout match
              case Some(StreamValue.Text(t)) => assert(t.contains("TaskList created"), s"create text: $t")
              case other => fail(s"expected text stdout, got $other")
            assertEquals(r2.status, Status.Done)
            assertEquals(r2.exit, Some(0))
            r2.finalStdout match
              case Some(StreamValue.Text(t)) =>
                assert(t.startsWith("TaskList"), s"list header: $t")
                assert(t.contains("#1"), s"#1 entry expected: $t")
              case other => fail(s"expected text stdout, got $other")
            // 落盘在套件级隔离根（beforeAll setDataRoot），非真实宿主 dataRoot
            assert(os.exists(PathUtil.dataRoot / "tasks.json"), "tasks.json must land in the isolated data root")
          }
        }
      }
    }

  test("真实执行·批 B 确定性负测四条：pop/nodeedit/taskboard/projectcreate（诚实拒答，零副作用）"):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        // ① pop：桥模板 agentDef=None ⇒ 身份闸 fail-closed ⇒ POP_NEBULA_ONLY
        val pop = run(h, Ir.Call("dev:tool:pop", JsonObject("filePath" -> "https://example.com".asJson)))
        // ② nodeedit：缺 project（且 ctx.projectName=None）⇒ resolveProject 确定性错误
        val nodeEdit =
          run(h, Ir.Call("dev:tool:nodeedit", JsonObject("nodename" -> "bridge-neg-node".asJson)))
        // ③ taskboard：resolveProject 先于身份闸 ⇒ 同款缺 project 锚
        val taskBoard = run(h, Ir.Call("dev:tool:taskboard", JsonObject("action" -> "list".asJson)))
        // ④ projectcreate：无 workspace 且桥模板 agentActorRef=None（/ headless）⇒ 明确报错，零写
        val before = walk(dataRootHome)
        val projectCreate = run(h, Ir.Call("dev:tool:projectcreate", JsonObject.empty))
        for
          rPop <- pop
          rNodeEdit <- nodeEdit
          rTaskBoard <- taskBoard
          rProjectCreate <- projectCreate
        yield
          List(rPop -> "pop", rNodeEdit -> "nodeedit", rTaskBoard -> "taskboard", rProjectCreate -> "projectcreate")
            .foreach { (r, label) =>
              assertEquals(r.status, Status.Done, s"$label status")
              assertEquals(r.exit, Some(1), s"$label exit")
              assertEquals(r.results.head.error.map(_.code), Some(Codes.CommandFailed), s"$label error code")
            }
          assert(
            rPop.results.head.error.exists(_.message.contains("POP_NEBULA_ONLY")),
            s"pop message=${rPop.results.head.error.map(_.message)}"
          )
          assert(
            rNodeEdit.results.head.error.exists(_.message.contains("Missing 'project' parameter")),
            s"nodeedit message=${rNodeEdit.results.head.error.map(_.message)}"
          )
          assert(
            rTaskBoard.results.head.error.exists(_.message.contains("Missing 'project' parameter")),
            s"taskboard message=${rTaskBoard.results.head.error.map(_.message)}"
          )
          // 「no interactive session available」或 headless 文案（两态均零写）
          assert(
            rProjectCreate.results.head.error.exists(_.message.contains("no interactive")),
            s"projectcreate message=${rProjectCreate.results.head.error.map(_.message)}"
          )
          // 零写：隔离根文件树逐字节不变（无 project scaffold / 无任何落盘）
          assertEquals(walk(dataRootHome), before, "projectcreate must not write anything")
        end for
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

  test("LLM 可见面钉（批 B 写面）：llm ingress 直连 ir 帧调 dev:tool:write ⇒ invalid + router.invalid_args(reason=audience)"):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        val plan = Ir.Call(
          "dev:tool:write",
          JsonObject("file_path" -> "/tmp/llm-side-door.txt".asJson, "content" -> "x".asJson)
        )
        run(h, plan, tenant = Tenant.Llm("sess-1", "agent-a"), ingress = Ingress.Llm).map { r =>
          assertEquals(r.status, Status.Invalid)
          assertEquals(r.exit, Some(2))
          assertEquals(r.error.map(_.code), Some(Codes.InvalidArgs))
          assertEquals(r.error.flatMap(_.details("reason")).flatMap(_.asString), Some("audience"))
        }
      }
    }

end IrToolBridgeSpec
