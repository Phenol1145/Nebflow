package nebflow.gateway

import cats.effect.{IO, Ref}
import io.circe.Json
import io.circe.syntax.*
import munit.CatsEffectSuite
import nebflow.core.tools.{ExternalToolConfig, ToolRegistry}
import nebflow.ir.*

/**
 * P1-4（ext: 过渡注册）一致性用例：声明级三层装载（直接构造 ExternalToolConfig，零
 * ScriptTool 依赖）+ 名字规范化/冲突拒绝 + reindex 幂等/摘除 + 执行面诚实拒答
 * （exit 126 + router.binding_unavailable）+ 单键空间不越界（TOOL_MAP/llmNameToIr）+
 * 人侧糖可达性。
 */
class IrExtBridgeSpec extends CatsEffectSuite:

  private val schema: io.circe.JsonObject =
    Json.obj("type" -> "object".asJson, "properties" -> Json.obj().asJson).asObject.get

  private def cfg(name: String, layer: String = "global", command: String = "echo hi"): ExternalToolConfig =
    ExternalToolConfig(
      name = name,
      description = s"test ext tool $name",
      command = command,
      inputSchema = schema,
      layer = layer
    )

  private def reindex(reg: CommandRegistry, configs: (ExternalToolConfig, os.Path)*): Unit =
    IrExtBridge.reindex(reg, configs.toList)

  private def extNames(reg: CommandRegistry): List[String] =
    reg.names.filter(_.startsWith("ext:")).sorted

  // ── ① 三层声明级 ──────────────────────────────────────────────────────

  test("① 三层声明级：ext:tool:x 在册且描述符=方案钉（binding 原文/caps 四件套/trust/audiences/llmName）"):
    val reg = new CommandRegistry()
    val dir = os.pwd
    reindex(
      reg,
      cfg("Deploy Helper", "global") -> dir,
      cfg("Team Tool", "team") -> dir,
      cfg("Flow Tool", "flow") -> dir
    )
    val g = reg.get("ext:tool:deploy_helper").getOrElse(fail("ext:tool:deploy_helper not registered"))
    assertEquals(g.binding, Binding.Node("Deploy Helper", "echo hi")) // N3：原始名与 command 原文存 binding
    assertEquals(g.caps, Set(Cap.Exec, Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Net("*")))
    assertEquals(g.pathArgs, Set.empty[String])
    assertEquals(g.params, Nil) // §9 红线 4
    assertEquals(g.trust, Trust.User)
    assertEquals(g.audiences, Set(Audience.Human)) // 过渡档
    assertEquals(g.llmName, Some("Deploy Helper")) // §7.5 迁移期锚 = 旧 ToolRegistry 键
    assertEquals(g.io, CommandIo(None, StreamKind.Text))
    assertEquals(reg.get("ext:tool:team_tool").map(_.trust), Some(Trust.Project))
    assertEquals(reg.get("ext:tool:flow_tool").map(_.trust), Some(Trust.Project))

  // ── ② 规范化与语法门 ──────────────────────────────────────────────────

  test("② 规范化：大写折叠/空格→_；含冒号多段合法；段>32 ⇒ 跳过不在册"):
    val reg = new CommandRegistry()
    val dir = os.pwd
    reindex(
      reg,
      cfg("UPPER Case") -> dir,
      cfg("a:b") -> dir,
      cfg("x" * 33) -> dir
    )
    assertEquals(reg.get("ext:tool:upper_case").isDefined, true)
    assertEquals(reg.get("ext:tool:a:b").isDefined, true)
    assertEquals(reg.get("ext:" + "x" * 33).isDefined, false)
    assertEquals(extNames(reg).exists(_.contains("xxxx")), false) // 非法名零进入（任何形态）

  // ── ③ 同档碰撞（N4）──────────────────────────────────────────────────

  test("③ 同档规范化碰撞：后件 N4 拒（先注册者留）"):
    val reg = new CommandRegistry()
    val dir = os.pwd
    // "Deploy Helper"（空格→_）与 "deploy_helper" 规范化后同名 ⇒ 同档（team=User 档外
    // 的 Project 档）碰撞 ⇒ 后件 N4 拒、先注册者留
    reindex(reg, cfg("Deploy Helper", "team") -> dir, cfg("deploy_helper", "team") -> dir)
    val cmd = reg.get("ext:tool:deploy_helper").getOrElse(fail("first registrant must stay"))
    assertEquals(cmd.binding, Binding.Node("Deploy Helper", "echo hi")) // 先者留
    assertEquals(extNames(reg), List("ext:tool:deploy_helper")) // 后者未进表

  // ── ④ 跨档覆盖（§7.4）────────────────────────────────────────────────

  test("④ 跨档：global+flow 同规范化名 ⇒ Project 覆盖（replaced）"):
    val reg = new CommandRegistry()
    val dir = os.pwd
    reindex(reg, cfg("Deploy Helper", "global") -> dir, cfg("deploy_helper", "flow") -> dir)
    val cmd = reg.get("ext:tool:deploy_helper").getOrElse(fail("cross-tier winner missing"))
    assertEquals(cmd.trust, Trust.Project) // 高档覆盖
    assertEquals(cmd.binding, Binding.Node("deploy_helper", "echo hi")) // 后写者生效

  // ── ⑤ reindex 幂等/摘除 ───────────────────────────────────────────────

  test("⑤ reindex 幂等/摘除：连调两次 names 不变；configs 变少 ⇒ 旧名已 unregister"):
    val reg = new CommandRegistry()
    val dir = os.pwd
    reindex(reg, cfg("Alpha") -> dir, cfg("Beta") -> dir)
    val names1 = extNames(reg)
    reindex(reg, cfg("Alpha") -> dir, cfg("Beta") -> dir)
    assertEquals(extNames(reg), names1)
    reindex(reg, cfg("Alpha") -> dir)
    assertEquals(extNames(reg), List("ext:tool:alpha"))
    assertEquals(reg.get("ext:tool:beta").isDefined, false)

  // ── ⑥ 执行面拒答（注册面存在、执行面诚实拒答）────────────────────────

  private def extRouter(root: os.Path, rules: List[PolicyRule]): (Router, Ref[IO, Vector[AuditRecord]]) =
    val reg = new CommandRegistry()
    reindex(reg, cfg("Ext Tool") -> root)
    val auditRef = Ref.unsafe[IO, Vector[AuditRecord]](Vector.empty)
    (new Router(reg, new PolicyEngine(PolicyConfig(rules = rules)), AuditSink.inMemory(auditRef)), auditRef)

  test("⑥ 执行面拒答：显式 Allow+ConfirmEdits ⇒ done/exit=126/binding_unavailable/message 含 registration-only"):
    IrTestKit.vfs().flatMap { root =>
      val (router, _) = extRouter(root, List(PolicyRule("ext:tool:ext_tool", Decision.Allow)))
      router
        .submit(IrTestKit.req(IrTestKit.call("ext:tool:ext_tool")), VfsRoot(root), Safety.ConfirmEdits)
        .map { r =>
          assertEquals(r.status, Status.Done)
          assertEquals(r.exit, Some(126))
          assertEquals(r.error.map(_.code), Some(Codes.BindingUnavailable))
          assertEquals(
            (r.error.map(_.message).getOrElse("") + r.error.map(_.details("reason").flatMap(_.asString).getOrElse("")))
              .contains("registration-only"),
            true
          )
        }
    }

  test("⑥b 无规则 ⇒ await_approval 零执行（四件套不⊆{FsRead} ⇒ default:no-rule Ask）"):
    IrTestKit.vfs().flatMap { root =>
      val (router, _) = extRouter(root, Nil)
      router
        .submit(IrTestKit.req(IrTestKit.call("ext:tool:ext_tool")), VfsRoot(root), Safety.ConfirmEdits)
        .map { r =>
          assertEquals(r.status, Status.AwaitApproval)
          assertEquals(r.exit, None)
          assertEquals(r.approval.map(_.rule), Some("default:no-rule"))
        }
    }

  test("⑥c AutoAll ⇒ 折 Allow 后进执行面吃 126 拒答（诚实，不假装执行）"):
    IrTestKit.vfs().flatMap { root =>
      val (router, _) = extRouter(root, Nil)
      router
        .submit(IrTestKit.req(IrTestKit.call("ext:tool:ext_tool")), VfsRoot(root), Safety.AutoAll)
        .map { r =>
          assertEquals(r.status, Status.Done)
          assertEquals(r.exit, Some(126))
          assertEquals(r.error.map(_.code), Some(Codes.BindingUnavailable))
        }
    }

  // ── ⑦ 单键空间不越界（桥纪律）────────────────────────────────────────

  test("⑦ 单键空间：TOOL_MAP 前后 diff 空；llmNameToIr 不含 ScriptTool 名/ext 名"):
    val before = ToolRegistry.TOOL_MAP.keySet.toSet
    val reg = new CommandRegistry()
    reindex(reg, cfg("Deploy Helper") -> os.pwd)
    assertEquals(ToolRegistry.TOOL_MAP.keySet.toSet, before) // 桥零 ToolRegistry 写
    assertEquals(IrLlmRoute.llmNameToIr.contains("Deploy Helper"), false) // ext: 不进 llm 改道表
    assertEquals(IrLlmRoute.llmNameToIr.values.exists(_.startsWith("ext:")), false)

  // ── ⑧ 人侧糖可达性 ───────────────────────────────────────────────────

  test("⑧ 人侧糖：/ext:tool:ext_tool --json {} 命中 lookup（Namespaces 含 ext）"):
    val reg = new CommandRegistry()
    reindex(reg, cfg("Ext Tool") -> os.pwd)
    // tokenizer 按空白切分（无引号语法）⇒ --json 取紧凑单 token {}
    ArgvSugar.lower("/ext:tool:ext_tool --json {}", reg.get) match
      case Left(err) => fail(s"sugar lowering failed: ${err.message}")
      case Right(call) =>
        assertEquals(call.name, "ext:tool:ext_tool")
        assertEquals(call.args.isEmpty, true) // --json '{}' ⇒ 空 object 绑定

end IrExtBridgeSpec
