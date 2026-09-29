package nebflow.gateway

import cats.effect.IO
import io.circe.syntax.*
import io.circe.{Json, JsonObject}
import munit.CatsEffectSuite
import nebflow.core.tools.{RemoteExecutor, ToolRegistry}
import nebflow.core.task.{FileTaskStore, TaskCreateInput, TaskStore}
import nebflow.ir.*
import nebflow.shared.PathUtil

/**
 * P1-2 批 A + 批 B + 批 C 桥接的一致性用例：声明级表驱动（**三十件全覆盖 / 双向完备 +
 * 未注册件反向断言（AskUserQuestion）** / 单键空间不越界 / 逐件精确 caps 集 / 机械
 * params 纪律 / 注册与幂等 reindex）+ 真实经 IR 执行（Read 真文件 / **写面正负两腿** /
 * tasklist 隔离根 / **批 C：bash 真执行 + teamtasklist 真读 + dangerousBash 组合
 * Ask 两腿** / 确定性负测组）+ LLM 面开钉（P1-3 audiences 翻转后：llm ingress 直连
 * ir 帧过受众面进策略面，ConfirmEdits 下 askRule 把守——await_approval/零执行，
 * 批 A/B/C 三面各一钉）。
 *
 * 批 B 写面核心验证点（缺一不可）：
 *  - 正腿：显式 Allow 规则（`dev:tool:write` 精确名）+ Safety.ConfirmEdits ⇒ 真写
 *    成功——证明放行来自规则而非 auto-all 折叠；
 *  - 负腿：harness rules=Nil（无 askRule）+ Safety.ConfirmEdits ⇒ `await_approval`
 *    且目标文件零变化——无规则时 FsWrite 走 `default:no-rule` 缺省 Ask（Policy 缺省
 *    分支），执行前短路 ⇒ 零副作用。
 *
 * 批 C 核心验证点（缺一不可）：
 *  - dangerousBash 组合两腿（任务书点名）：`Allow(dev:tool:bash)` 精确名 +
 *    ConfirmEdits 下——危险命令（`rm -rf …`）被组合面硬底拉回 `await_approval`
 *    （rule=`bash:danger`、零执行）；安全命令（`echo`）放行且真 BashTool 执行
 *    （final.text 含 marker 与恒存的 `(cwd:` 行）——两腿合证；
 *  - 高能力面真实执行（任务书点名）：`dev:tool:teamtasklist` 经 `taskStore` 模板
 *    槽直读 FileTaskStore（套件级隔离 dataRoot + 独有 team scope，hwm 单例无跨套
 *    污染——TaskStore.scala 进程级 hwmRef 注记）；
 *  - 确定性负测组：subtask/teamtaskcreate/load-team/delegate(None lib)/sendmessage
 *    ——诚实拒答文案 + 零副作用（「注册面存在、执行面诚实拒答」形态，批 B 先例）；
 *  - 未注册件反向断言：AskUserQuestion 缺 `agentActorRef` 槽（两模式必死路）⇒ 不注册
 *    （IrToolCaps.bridged 无行 / defs 无名 / ToolRegistry 本体不动）。
 *
 * caps 断言是**精确集合相等**（既是下限也是上限）：glob/grep 退回仅 FsRead（漏报
 * Exec）必红；nodeedit 漏报 Exec 同红；给任一件塞多余 cap 也红。
 *
 * 套件级 `PathUtil.setDataRoot` 隔离（TaskListToolSpec 同款配方）：TaskList 真实写
 * 腿的存储根是全局 `dataRoot`（非 VFS root），不换根会污染真实宿主 tasks.json。
 */
class IrToolBridgeSpec extends CatsEffectSuite:

  // lazy on purpose：MemoryNoteTool.inputSchema 是 def 且描述文本内嵌 dataRoot 派生
  // 路径——val 会在类构造期（beforeAll 的 setDataRoot 之前）冻结默认根路径，导致
  // 与测试期重算的 augmentSchema 逐字节比较失败；lazy 使两侧同在隔离根下求值。
  private lazy val defs: List[CommandDef] = IrToolBridge.defs(None)

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

  // ── 声明级：三十件全覆盖 / 命名与 binding ──────────────────────

  test("表驱动·三十件全覆盖：恰 30 件，irName 集合精确相等，语法/首段/binding 全过"):
    assertEquals(defs.length, 30)
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
        "dev:tool:taskboard",
        // 批 C 高能力面十五件（同机械命名规则；AskUserQuestion 不注册——见反向断言腿）
        "dev:tool:bash",
        "dev:tool:websearch",
        "dev:tool:webfetch",
        "dev:tool:curl",
        "dev:tool:mail",
        "dev:tool:sendmessage",
        "dev:tool:delegate",
        "dev:tool:subtask",
        "dev:tool:memorynote",
        "dev:tool:schedule",
        "dev:tool:teamtaskcreate",
        "dev:tool:teamtaskupdate",
        "dev:tool:teamtasklist",
        "dev:tool:load",
        "dev:tool:agentcontrol"
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
    // 批 C·未注册件反向断言三连（AskUserQuestion——缺 agentActorRef 槽，两模式必然
    // 运行时失败，缺席优于假门）：桥表无行 / defs 无名 / 工具本体仍在 ToolRegistry
    // （LLM 面零扰动）
    assert(!rows.exists(_.toolName == "AskUserQuestion"), "AskUserQuestion must NOT be in the bridge table")
    assert(defs.forall(_.name != "dev:tool:askuserquestion"), "dev:tool:askuserquestion must NOT be defined")
    assert(ToolRegistry.TOOL_MAP.contains("AskUserQuestion"), "the tool itself stays in ToolRegistry untouched")

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
    // 批 C 补第六件 bash；其余二十四件不带
    List("dev:tool:read", "dev:tool:write", "dev:tool:edit", "dev:tool:glob", "dev:tool:grep", "dev:tool:bash")
      .foreach { n =>
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
      "dev:tool:taskboard",
      // 批 C 其余十四件（Bash 之外全不带）
      "dev:tool:websearch",
      "dev:tool:webfetch",
      "dev:tool:curl",
      // dev:tool:mail 不在此列：Mail 自有 device 属性（设备腿地址面，schema 原生键，
      // 非 augmentSchema 注入——注入面断言对它无意义）
      "dev:tool:sendmessage",
      "dev:tool:delegate",
      "dev:tool:subtask",
      "dev:tool:memorynote",
      "dev:tool:schedule",
      "dev:tool:teamtaskcreate",
      "dev:tool:teamtaskupdate",
      "dev:tool:teamtasklist",
      "dev:tool:load",
      "dev:tool:agentcontrol"
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
      "dev:tool:taskboard" -> Set(Cap.FsWrite("*")),
      // 批 C 十五件（命令面不可静态判定/传递性/侦察修正处见 IrToolCaps 行内注；
      // agentcontrol 无文件/网络/spawn 声明面 ⇒ 空集）
      "dev:tool:bash" -> Set(Cap.Exec, Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Net("*")),
      "dev:tool:websearch" -> Set(Cap.Net("*")),
      "dev:tool:webfetch" -> Set(Cap.Net("*")),
      "dev:tool:curl" -> Set(Cap.Net("*")),
      "dev:tool:mail" -> Set(Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Net("*")),
      "dev:tool:sendmessage" -> Set(Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Net("*")),
      "dev:tool:delegate" -> Set(Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Exec, Cap.Net("*")),
      "dev:tool:subtask" -> Set(Cap.FsRead("*"), Cap.FsWrite("*"), Cap.Exec, Cap.Net("*")),
      "dev:tool:memorynote" -> Set(Cap.MemoryWrite, Cap.FsWrite("*")),
      "dev:tool:schedule" -> Set(Cap.FsWrite("*")),
      "dev:tool:teamtaskcreate" -> Set(Cap.FsWrite("*")),
      "dev:tool:teamtaskupdate" -> Set(Cap.FsWrite("*")),
      "dev:tool:teamtasklist" -> Set(Cap.FsRead("*")),
      "dev:tool:load" -> Set(Cap.FsRead("*"), Cap.FsWrite("*")),
      "dev:tool:agentcontrol" -> Set.empty[Cap]
    )
    defs.foreach { cmd =>
      assertEquals(cmd.caps, expected(cmd.name), s"caps of ${cmd.name}")
    }
    val known = PolicyConfig().knownCapKinds
    defs.foreach { cmd =>
      assert(cmd.capKinds.forall(known.contains), s"cap kinds of ${cmd.name} must be known to the engine")
    }

  test("表驱动·pathArgs/io/audiences/llmName：三十件统一（pathArgs=∅、io=Text、{Human,Llm}、llmName=旧名）"):
    defs.foreach { cmd =>
      assertEquals(cmd.pathArgs, Set.empty[String], s"pathArgs of ${cmd.name}")
      assertEquals(cmd.io, CommandIo(stdin = None, stdout = StreamKind.Text), s"io of ${cmd.name}")
      // P1-3（audiences 翻转）：{Human} → {Human,Llm}——不翻则 llm ingress 对 dev:tool:*
      // 被 C21 拒、改道死胎；激活 §7.5 llmName 必填+全局唯一注册期校验（桥已带旧名）
      assertEquals(cmd.audiences, Set(Audience.Human, Audience.Llm), s"audiences of ${cmd.name}")
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
    // 批 C 锚①：bash 糖面恰 {command, description, background_job_id, device}——
    // timeout（number）/run_in_background、persistent、cancel_background_job（boolean）
    // 不进；device（string，remoteable 注入）机械进糖
    val bash = byName("dev:tool:bash").params.map(_.name).toSet
    assertEquals(bash, Set("command", "description", "background_job_id", "device"))
    // 批 C 锚②：mail 含 {address, device, message, type, chainId}（string 全进），
    // images/attachments（array）不进
    val mail = byName("dev:tool:mail").params.map(_.name).toSet
    assertEquals(mail, Set("address", "device", "message", "type", "chainId"))
    // 批 C 锚③：sendmessage 含 {to, message, targetDir} 不含 overwrite（boolean）/
    // attachments（array）
    val send = byName("dev:tool:sendmessage").params.map(_.name).toSet
    assertEquals(send, Set("to", "message", "targetDir"))
    // 批 C 锚④：memorynote 五串键全进
    assertEquals(
      byName("dev:tool:memorynote").params.map(_.name).toSet,
      Set("target", "action", "section", "match", "content")
    )
    // 批 C 锚⑤：teamtaskcreate 不含 blockedBy（array）；teamtasklist 糖面恰 {team, status}
    val teamCreate = byName("dev:tool:teamtaskcreate").params.map(_.name).toSet
    assertEquals(teamCreate.intersect(Set("blockedBy")), Set.empty[String])
    assertEquals(byName("dev:tool:teamtasklist").params.map(_.name).toSet, Set("team", "status"))
    // 批 C 锚⑥：schedule 的 triggerAt（type=[integer,string] 数组形态 ⇒ admitsString
    // 不认）不进糖；action/content/repeat/name/id（string）进——机械规则是唯一裁决面
    val schedule = byName("dev:tool:schedule").params.map(_.name).toSet
    assertEquals(schedule, Set("action", "content", "repeat", "name", "id"))
    // 批 C 锚⑦：agentcontrol 的 confirm（boolean）不进，糖面恰 {action, sessionId,
    // reason}；load 糖面恰 {type, name}
    assertEquals(byName("dev:tool:agentcontrol").params.map(_.name).toSet, Set("action", "sessionId", "reason"))
    assertEquals(byName("dev:tool:load").params.map(_.name).toSet, Set("type", "name"))
    // 批 C 锚⑧：三件 Net 面——websearch 恰 {query, engine}（max_results number、
    // allowed/blocked_domains array 不进）；webfetch 恰 {url, format}；curl 恰
    // {url, method, body}（headers object、timeout number 不进）
    assertEquals(byName("dev:tool:websearch").params.map(_.name).toSet, Set("query", "engine"))
    assertEquals(byName("dev:tool:webfetch").params.map(_.name).toSet, Set("url", "format"))
    assertEquals(byName("dev:tool:curl").params.map(_.name).toSet, Set("url", "method", "body"))

  // ── 注册与幂等 reindex ──────────────────────────────────────

  test("注册与幂等：三十件注册全 Right；同档重注册=N4 碰撞；reindex 连跑两次不丢件"):
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
      // 幂等 reindex ×2：三十件仍在，名集恰为 fs 两件 + tool 三十件
      IrToolBridge.reindex(reg, None)
      IrToolBridge.reindex(reg, None)
      assertEquals(reg.names.toSet, (DevCommands.base.map(_.name) ++ defs.map(_.name)).toSet)
      assert(reg.get("dev:tool:read").isDefined)
      assert(reg.get("dev:tool:write").isDefined)
      assert(reg.get("dev:tool:taskboard").isDefined)
      assert(reg.get(IrToolCaps.bashIrName).isDefined, "dev:tool:bash must survive reindex")
      assert(reg.get("dev:tool:teamtasklist").isDefined, "dev:tool:teamtasklist must survive reindex")
      assert(reg.get("dev:tool:agentcontrol").isDefined, "dev:tool:agentcontrol must survive reindex")
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

  test(
    "LLM 面开钉（P1-3 audiences 翻转）：llm ingress 直连 ir 帧调 dev:tool:read ⇒ 过受众面进策略面，ConfirmEdits 下 await_approval（rule=bridge:opaque-host、零执行）"
  ):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        val plan = Ir.Call("dev:tool:read", JsonObject("file_path" -> "/etc/hosts".asJson))
        run(h, plan, tenant = Tenant.Llm("sess-1", "agent-a"), ingress = Ingress.Llm, safety = Safety.ConfirmEdits)
          .map { r =>
            assertEquals(r.status, Status.AwaitApproval)
            assertEquals(r.exit, None)
            assertEquals(r.results, Nil) // 零执行
            val ap = r.approval.getOrElse(fail("approval body missing"))
            assertEquals(ap.nodes.map(_.command), List("dev:tool:read"))
            assertEquals(ap.rule, "bridge:opaque-host")
          }
      }
    }

  test("LLM 面开钉（写面，P1-3）：llm ingress 直连 ir 帧调 dev:tool:write ⇒ 策略 Ask 把守（await_approval、零执行）"):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        val plan = Ir.Call(
          "dev:tool:write",
          JsonObject("file_path" -> "/tmp/llm-side-door.txt".asJson, "content" -> "x".asJson)
        )
        run(h, plan, tenant = Tenant.Llm("sess-1", "agent-a"), ingress = Ingress.Llm, safety = Safety.ConfirmEdits)
          .map { r =>
            assertEquals(r.status, Status.AwaitApproval)
            assertEquals(r.results, Nil)
            assertEquals(r.approval.map(_.rule), Some("bridge:opaque-host"))
            assertEquals(os.exists(os.Path("/tmp/llm-side-door.txt", os.pwd)), false)
          }
      }
    }

  // ── 真实执行·批 C 高能力面（任务书点名项：dangerousBash 组合 + 真执行闭环）────

  test(
    "批 C·dangerousBash 组合两腿：Allow(dev:tool:bash)+ConfirmEdits 下危险命令被硬底拉回 Ask（rule=bash:danger、零执行），安全命令放行且真执行"
  ):
    IrTestKit.vfs().flatMap { root =>
      // 组合面装配 = IrGateway 生产装配的同构注入：谓词单源 IrToolBridge.bashDanger
      // （BashTool.isDangerous），匹配名集 = Set(bashIrName)；规则 = 精确名 Allow ::
      // askRules（Allow 在前，rules.find 首个命中生效——证明安全腿放行来自显式规则）
      IrTestKit
        .harness(
          root,
          extra = defs,
          policyCfg = Some(
            PolicyConfig(
              rules = PolicyRule(name = IrToolCaps.bashIrName, decision = Decision.Allow) :: IrToolCaps.askRules,
              dangerousBash = IrToolBridge.bashDanger,
              dangerousBashNames = Set(IrToolCaps.bashIrName)
            )
          )
        )
        .flatMap { h =>
          val danger =
            run(
              h,
              Ir.Call(IrToolCaps.bashIrName, JsonObject("command" -> "rm -rf /tmp/ir-bridge-danger".asJson)),
              safety = Safety.ConfirmEdits
            )
          val safe =
            run(
              h,
              Ir.Call(IrToolCaps.bashIrName, JsonObject("command" -> "echo ir-safe".asJson)),
              safety = Safety.ConfirmEdits
            )
          for
            rDanger <- danger
            rSafe <- safe
            records <- h.records
          yield
            // 危险腿：Allow+危险 ⇒ Ask（combine 只升不降），rule 逐字 = "bash:danger"，
            // 执行前短路 ⇒ 零执行
            assertEquals(rDanger.status, Status.AwaitApproval)
            assertEquals(rDanger.exit, None)
            assertEquals(rDanger.results, Nil)
            val ap = rDanger.approval.getOrElse(fail("approval body missing"))
            assertEquals(ap.nodes.map(_.command), List(IrToolCaps.bashIrName))
            assertEquals(ap.rule, "bash:danger")
            // 安全腿：显式 Allow 放行 + 非危险 ⇒ 真 BashTool 执行（先例 BashToolSpec 真
            // echo 同环境）；final.text 含 marker 与 formatResult 恒存 (cwd: 行
            assertEquals(rSafe.status, Status.Done, s"error=${rSafe.error.map(_.message)}")
            assertEquals(rSafe.exit, Some(0))
            rSafe.finalStdout match
              case Some(StreamValue.Text(t)) =>
                assert(t.contains("ir-safe"), s"echo marker expected: $t")
                assert(t.contains("(cwd:"), s"cwd line expected: $t")
              case other => fail(s"expected text stdout, got $other")
            // 审计：危险腿 ask/bash:danger + 安全腿 allow（决策来自规则与组合面，非档位）
            assertEquals(
              records.map(r => r.command -> r.decision).toSet,
              Set(IrToolCaps.bashIrName -> "ask", IrToolCaps.bashIrName -> "allow")
            )
            assert(records.find(_.decision == "ask").exists(_.rule == Some("bash:danger")))
            assert(records.forall(r => !r.toJson.asObject.get.contains("args")))
          end for
        }
    }

  test("批 C·高能力面真实执行①：dev:tool:teamtasklist 真读 FileTaskStore（taskStore 模板槽 + 显式 team 两机制闭环）"):
    IrTestKit.vfs().flatMap { root =>
      // 独有 team scope（TaskStore 进程级 hwmRef 单例注记：跨套件必须用独有名）；
      // FileTaskStore.root 是 def ⇒ 动态取套件级隔离 dataRoot
      bridgedHarness(root).flatMap { h =>
        FileTaskStore
          .create(
            TaskStore.teamScopeKey("ir-bridge-batchc"),
            TaskCreateInput(
              subject = "batch-c bridge entry",
              description = "seeded by IrToolBridgeSpec"
            )
          )
          .flatMap { _ =>
            val plan = Ir.Call("dev:tool:teamtasklist", JsonObject("team" -> "ir-bridge-batchc".asJson))
            run(h, plan).map { r =>
              assertEquals(r.status, Status.Done, s"error=${r.error.map(_.message)}")
              assertEquals(r.exit, Some(0))
              r.finalStdout match
                case Some(StreamValue.Text(t)) =>
                  assert(t.contains("Team tasks for 'ir-bridge-batchc' (1)"), s"list text: $t")
                  assert(t.contains("#1"), s"#1 entry expected: $t")
                case other => fail(s"expected text stdout, got $other")
            }
          }
      }
    }

  test("批 C·高能力面真实执行②：dev:tool:bash echo 经 IR 全链真执行（前台腿，final.text 含 marker 与 (cwd: 行）"):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        val plan = Ir.Call(IrToolCaps.bashIrName, JsonObject("command" -> "echo ir-bridge-batchc".asJson))
        run(h, plan).map { r =>
          assertEquals(r.status, Status.Done, s"error=${r.error.map(_.message)}")
          assertEquals(r.exit, Some(0))
          r.finalStdout match
            case Some(StreamValue.Text(t)) =>
              assert(t.contains("ir-bridge-batchc"), s"echo marker expected: $t")
              assert(t.contains("(cwd:"), s"cwd line expected (formatResult恒存): $t")
            case other => fail(s"expected text stdout, got $other")
        }
      }
    }

  test("批 C·确定性负测组：subtask/teamtaskcreate/load-team/delegate/sendmessage（诚实拒答，零副作用）"):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        // defs(None) ⇒ lib=None ⇒ delegate 走确定性拒腿（生产 Some(agentLibrary) 才真实可用）
        val before = walk(dataRootHome)
        val subTask =
          run(h, Ir.Call("dev:tool:subtask", JsonObject("prompt" -> "x".asJson, "description" -> "d".asJson)))
        val teamCreate = run(
          h,
          Ir.Call(
            "dev:tool:teamtaskcreate",
            JsonObject("subject" -> "s".asJson, "description" -> "d".asJson)
          )
        )
        val load = run(h, Ir.Call("dev:tool:load", JsonObject("type" -> "team".asJson, "name" -> "no-such".asJson)))
        val delegate =
          run(h, Ir.Call("dev:tool:delegate", JsonObject("task" -> "t".asJson, "description" -> "d".asJson)))
        // sendmessage 走 **device 腿**：好友腿的可用性取决于 FriendMessageTool.service
        // 进程级单例——neblink 域套件（FriendMessageToolSpec/FriendAttachGateSpec）会
        // initialize 它,全量回归下本套件晚于其运行 ⇒ 好友腿结局随套件次序漂移（全量
        // 回归实测翻红）。device 腿只读 ctx.sharedResources（桥模板 sr=None ⇒
        // resources=None）——只依赖模板,不依赖任何全局状态,任意次序下确定性拒答。
        val send =
          run(
            h,
            Ir.Call("dev:tool:sendmessage", JsonObject("to" -> "device:no-such".asJson, "message" -> "hi".asJson))
          )
        for
          rSubTask <- subTask
          rTeamCreate <- teamCreate
          rLoad <- load
          rDelegate <- delegate
          rSend <- send
        yield
          List(
            rSubTask -> "subtask",
            rTeamCreate -> "teamtaskcreate",
            rLoad -> "load",
            rDelegate -> "delegate",
            rSend -> "sendmessage"
          )
            .foreach { (r, label) =>
              assertEquals(r.status, Status.Done, s"$label status")
              assertEquals(r.exit, Some(1), s"$label exit")
              assertEquals(r.results.head.error.map(_.code), Some(Codes.CommandFailed), s"$label error code")
            }
          // 拒答文案逐件（各自述缺失的身份/槽位/服务面）
          assert(
            rSubTask.results.head.error.exists(_.message.contains("No agent definition available")),
            s"subtask message=${rSubTask.results.head.error.map(_.message)}"
          )
          assert(
            rTeamCreate.results.head.error.exists(_.message.contains("only available to team agents")),
            s"teamtaskcreate message=${rTeamCreate.results.head.error.map(_.message)}"
          )
          // load-team：文件不存在腿先拦（agentActorRef 缺失腿另证——两条腿都是 exit 1 零副作用）
          assert(
            rLoad.results.head.error.exists(_.message.contains("Team file not found")),
            s"load message=${rLoad.results.head.error.map(_.message)}"
          )
          assert(
            rDelegate.results.head.error.exists(_.message.contains("No agent library available")),
            s"delegate message=${rDelegate.results.head.error.map(_.message)}"
          )
          assert(
            rSend.results.head.error.exists(_.message.contains("Device messaging is unavailable")),
            s"sendmessage message=${rSend.results.head.error.map(_.message)}"
          )
          // 零副作用：隔离根文件树逐字节不变（无 spawn/无 team task 落盘/无网络外呼）
          assertEquals(walk(dataRootHome), before, "negative group must not write anything")
        end for
      }
    }

  test("LLM 面开钉（批 C 高能力面，P1-3）：llm ingress 直连 ir 帧调 dev:tool:bash ⇒ 策略 Ask 把守（await_approval、零执行）"):
    IrTestKit.vfs().flatMap { root =>
      bridgedHarness(root).flatMap { h =>
        val plan = Ir.Call(IrToolCaps.bashIrName, JsonObject("command" -> "echo side-door".asJson))
        run(h, plan, tenant = Tenant.Llm("sess-1", "agent-a"), ingress = Ingress.Llm, safety = Safety.ConfirmEdits)
          .map { r =>
            assertEquals(r.status, Status.AwaitApproval)
            assertEquals(r.results, Nil) // 最危险的件同样零执行（Ask 先于一切副作用）
            assertEquals(r.approval.map(_.rule), Some("bridge:opaque-host"))
          }
      }
    }

end IrToolBridgeSpec
