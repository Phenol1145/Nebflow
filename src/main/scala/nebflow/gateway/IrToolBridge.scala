/* 命令 IR 路由层 P1-2 批 A（只读七件）+ 批 B（写面八件）+ 批 C（高能力面十五件）：
 * 内置 Tool → IR 命令的**桥本体**（表 → CommandDef 装配 + DevHandler 四面适配 +
 * 幂等 reindex）。
 *
 * 单键空间纪律（§7.5 并存期铁律）：本桥**只读** `ToolRegistry.TOOL_MAP` 查实例，
 * 零 `registerTool`/`unregisterTool` 调用——`dev:tool:*` 名只存在于 IR 侧
 * `CommandRegistry`，LLM 面工具表（`buildToolList`/`buildAllowedToolSet`/ALL_TOOLS）
 * 逐字节不变。P1-3（audiences 翻转）：`audiences={Human,Llm}` + `llmName=旧 Tool 名`
 * ——llm ingress 经 `executeTool` 第三前置改道闸（`IrRoutePort`）派发进 IR，
 * §7.4 检查点①对**改道腿**放行；模型面工具名与工具表零扰动（D30：开关只改派发面）。
 * 执行腿双形态：fiber-local 置位（llm 改道腿）⇒ 活闭包（见 [[llmExecLocal]]）；未置位
 * （人侧糖腿/ws 直连 ir 帧）⇒ 静态模板逐字节不变。
 *
 * 四面适配（`DevHandler = (JsonObject, CallCtx) => IO[Either[IrError, StreamValue]]`）：
 *  ① 参数键透传：args 原样交 `tool.call(input=args)`——键面 = 工具自己的
 *    inputSchema（§7.6 唯一权威；`file_path`/`node-id` 等键原样，不做 camelCase 别名层）；
 *  ② ToolContext 模板 + 回填：模板 = 残缺构造先例同款（`WsSessionChatHandlers` 的
 *    AgentControl 直调点）——`projectRoot=""`、`sharedResources=Some(sr)`、
 *    `actorSystem=Some(sr.actorSystem)`；每调用回填 `sessionId`/`requestId`（CallCtx），
 *    其余槽位缺省（`sandbox=off` ⇒ Read 拒相对路径、Glob/Grep 相对根 = user.dir——
 *    与 REST 直调旧行为同款，诚实不发明会话根解析）。批 B 写面同款诚实降级：
 *    `fileHistory`/`fileLockManager`/`readTracker`/`fileChangeTracker`/`wsSend`/
 *    `agentActorRef`/`agentDef`/`isDispatcher`/`flowNodeId` 全 None/false ⇒ Write/Edit
 *    的 Option-traverse 全 no-op（无快照/无锁/无 WS 帧/无记忆通知，MemoryChangeNotifier
 *    同步链），Pop/TaskBoard 身份闸 fail-closed，ProjectCreate 无 actor 面——与 REST
 *    直调旧行为一致，**不发明会话根/身份/WS 通道**。批 C 模板恰补两槽：
 *    `taskStore=Some(FileTaskStore)`（生产 ToolContext 同源 `GatewayMain`，TeamTaskList
 *    真读腿）与 `agentLibrary=lib`（`initBridge` 新参，Delegate 真实可用；两槽经实测
 *    grep 对批 A/B 十五件零读取、零行为差）；其余槽位仍 None/off/false（Bash 后台
 *    无 WS 指示/无完成通知但可查、Schedule/TeamTask 面板帧 no-op、SubTask/
 *    TeamTaskCreate/Update 身份闸确定性拒答——「注册面存在、执行面诚实拒答」形态）；
 *  ③ 结果映射：`Right(s) → StreamValue.Text(s)`；`Left(ToolError(m)) →
 *    IrError.commandFailed(m)`（§12 迁移口径：exit 1 + command.failed，**禁止**把工具
 *    失败伪装成路由层错误）；异常兜底同码（`DevCommands` handleErrorWith 先例）；
 *  ④ J6：三十件声明 `io.stdout = Text` 且适配器只产 `StreamValue.Text` ⇒ 执行器
 *    声明/实况闸恒过（text 1MiB 限额失败不截断是 §5.2 规范行为）。批 B 注记：Card
 *    结果 = `___CARD_HTML___` + JSON 载荷（本地引用已内联），是 JSON 文本 ⇒ Text 恒符；
 *    大卡片超 1MiB 按规范失败（诚实，不截断）。
 *
 * 批 C 另产出 [[bashDanger]]：`PolicyConfig.dangerousBash` 的装配谓词（单源 =
 * `BashTool.isDangerous`，不在 gateway 重写模式表）——`IrGateway` 装配面接通
 * `dev:tool:bash` 的危险命令组合底座（`Policy.dangerousBashNames` 匹配面，批 C 扩展）。
 */
package nebflow.gateway

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, IOLocal}
import io.circe.{Json, JsonObject}
import nebflow.core.AgentRuntimePort
import nebflow.core.AgentLibraryView
import nebflow.core.task.FileTaskStore
import nebflow.core.tools.{RemoteExecutor, Tool, ToolContext, ToolRegistry}
import nebflow.ir.*
import nebflow.shared.NebflowLogger

object IrToolBridge:

  private val logger = NebflowLogger.forName("nebflow.ir.bridge")

  /**
   * P1-3（LLM ingress 改道）：llm 腿的 fiber-local **活闭包靶**。置位 ⇒ handler 不走
   * 静态模板，改调 [[IrExec.run]]（= 调用方 executeTool 闭包，内含 hook/沙箱/快照/
   * wsSend/modelFacingResult/extractImages 全部旧引擎纵深）；未置位（人侧糖腿 / ws
   * 直连 ir 帧）⇒ 静态模板逐字节不变。`IOLocal`：CE3 fiber 作用域（先例
   * `agent/SendConfirm.scala:93-94`）——route→submit→handler 同 fiber，parTraverse 对
   * 每元素 fork 各持一份，并发零串台；`locally` 用 getAndSet+bracket 保证异常路径还原。
   */
  private val llmExecLocal: IOLocal[Option[IrExec]] =
    IOLocal[Option[IrExec]](None).unsafeRunSync()

  /** llm 腿置靶唯一入口：本次 submit 期间挂活闭包（退出即还原上一值，可嵌套）。 */
  private[gateway] def llmLocally[A](exec: IrExec)(io: IO[A]): IO[A] =
    llmExecLocal.getAndSet(Some(exec)).bracket(_ => io)(prev => llmExecLocal.set(prev).void)

  /** 糖可用形参名：`--<key>` 的 key 必须是合法 flag 名（首字符字母）——排除 `-i`/`-A` 这类 dash 属性。 */
  private val FlagNameRe = "^[A-Za-z][A-Za-z0-9_-]*$".r

  /**
   * 危险 bash 命令谓词（批 C，`PolicyConfig.dangerousBash` 装配面）：取 `args.command`
   * 过 `BashTool.isDangerous`（公开单源——固定危险模式 + dataRoot 抹除面，即既有审批
   * 闸集，不在 gateway 重写）。command 键缺席/非串 ⇒ 不危险（非命令面调用，如
   * background_job_id 查询腿——查询不执行新命令）。
   */
  val bashDanger: (String, JsonObject) => Boolean = (_, args) =>
    args("command").flatMap(_.asString).exists(nebflow.core.tools.BashTool.isDangerous)

  /** `dev:tool:*` 命令定义（声明级；`sr`/`lib` 只进 handler，不进描述符面）。缺件（ToolRegistry 无该名）跳过并 WARN。 */
  def defs(sr: Option[AgentRuntimePort], lib: Option[AgentLibraryView] = None): List[CommandDef] =
    IrToolCaps.bridged.flatMap(row =>
      ToolRegistry.TOOL_MAP.get(row.toolName) match
        case Some(tool) => Some(toCommandDef(row, tool, sr, lib))
        case None =>
          logger.warnSync(s"bridge: builtin tool '${row.toolName}' not in ToolRegistry — skipping ${row.irName}")
          None
    )

  /**
   * 幂等 reindex（先摘后挂）：同档重注册 = N4 碰撞拒绝（`CommandRegistry.install`），
   * 重装载必须先 `unregister` 再 `register`。可重入：重复调用结果一致。
   * 单件注册失败（Left）记 ERROR 跳过该件，不炸整批。表变更假定发生在 boot 单 fiber
   * （`LinkedHashMap` 并发写保护是热重载批次的事）。
   */
  def reindex(registry: CommandRegistry, sr: Option[AgentRuntimePort], lib: Option[AgentLibraryView] = None): Unit =
    IrToolCaps.bridged.foreach { row =>
      registry.unregister(row.irName)
      ToolRegistry.TOOL_MAP.get(row.toolName) match
        case Some(tool) =>
          registry.register(toCommandDef(row, tool, sr, lib)) match
            case Left(err) =>
              logger.errorSync(s"bridge: registering ${row.irName} failed: ${err.message} — skipped")
            case Right(_) => ()
        case None =>
          logger.errorSync(s"bridge: builtin tool '${row.toolName}' missing from ToolRegistry — ${row.irName} skipped")
    }

  /** 一行声明表 + 工具实例 → `CommandDef`（机械装配面，字段来源见行内注释）。 */
  private def toCommandDef(
    row: Bridged,
    tool: Tool,
    sr: Option[AgentRuntimePort],
    lib: Option[AgentLibraryView]
  ): CommandDef =
    // argsSchema = augmentSchema 后的 inputSchema（与 ALL_TOOLS 同一出口，保持 device 面：
    // Read/Glob/Grep 的 remoteable schema 带 device 键；桥本地面不接远端派发，该键对
    // 工具 inert——透传不删，保持与 LLM 面 schema 机械一致）
    val schema = RemoteExecutor.augmentSchema(tool.name, tool.inputSchema)
    CommandDef(
      name = row.irName,
      description = row.description,
      argsSchema = schema,
      binding = Binding.Dev(handler(tool, sr, lib)),
      io = IrToolCaps.io,
      pathArgs = IrToolCaps.pathArgs,
      caps = row.caps,
      params = paramsFrom(schema),
      delivery = Delivery.Both,
      trust = IrToolCaps.trust,
      audiences = IrToolCaps.audiences,
      // §7.5 迁移期锚：llmName = 旧 Tool 名原样（不受 §7.3 命名语法约束；本批
      // audiences 不含 Llm ⇒ 校验面零触发，纯元数据锚，防 IR 名推导漂移）
      llmName = Some(row.toolName)
    )

  end toCommandDef

  /**
   * 人侧 argv 糖形参的**机械生成**（P1-2 硬约束：全 named 无 positional）。规则单一、
   * 无逐件手工清单：
   *  - 名字须是合法 flag 键（`^[A-Za-z][a-zA-Z0-9_-]*$`）——`-i`/`-A`/`-B`/`-C` 这类
   *    dash 属性不进糖（只留 `--json` 通道）；
   *  - 属性须**能以 JSON 字符串绑定**：`type=string`，或无 `type` 而 `oneOf` 含 string
   *    分支（NodeList.status 的逗号串形态）。number/integer/boolean 属性不进糖——糖值
   *    恒字符串（ArgvSugar），绑 number 必然 exit 2，不如糖面直接 unknown_flag 指路 --json；
   *  - `required` 从 schema 的 required 数组机械镜像；`default` 不注入（[D19]）。
   */
  private def paramsFrom(schema: JsonObject): List[Param] =
    val required = schema("required")
      .flatMap(_.asArray)
      .map(_.flatMap(_.asString).toSet)
      .getOrElse(Set.empty[String])
    schema("properties").flatMap(_.asObject).map(_.toIterable.toList).getOrElse(Nil).collect {
      case (key, prop) if FlagNameRe.matches(key) && admitsString(prop) =>
        Param(
          name = key,
          positional = None,
          required = required.contains(key),
          default = None,
          isFlag = false,
          description = prop.asObject.flatMap(_("description")).flatMap(_.asString).getOrElse("")
        )
    }

  end paramsFrom

  /** 属性是否可被糖以字符串绑定（见 [[paramsFrom]] 的规则说明）。 */
  private def admitsString(prop: Json): Boolean =
    val obj = prop.asObject.getOrElse(JsonObject.empty)
    obj("type").flatMap(_.asString) match
      case Some("string") => true
      case Some(_) => false
      case None =>
        obj("oneOf")
          .flatMap(_.asArray)
          .exists(_.exists { branch =>
            branch.asObject.exists(b => b("type").flatMap(_.asString).contains("string"))
          })

  /**
   * ② 的模板 + 回填：模板一次构造，回填每调用（sessionId/requestId 来自 CallCtx）。
   * 批 C 补两槽：`taskStore=Some(FileTaskStore)`（进程级单例，生产 ToolContext 同源
   * `GatewayMain.startMcpServers` 的 sharedResources 装配）与 `agentLibrary=lib`
   * （`initBridge` 注入；None ⇒ Delegate 确定性拒 "No agent library available"）。
   *
   * P1-3 fiber-local 分支：`llmExecLocal` 置位（llm 改道腿，经 [[llmLocally]]）⇒ **活
   * 闭包**执行——`exec.run()` 即调用方的 executeToolInner，结果映射 isError⇒
   * `Left(commandFailed(content))` / else⇒ `Right(Text(content))`（content=闭包模型面
   * 串原样，保证 IR 契约面的 content 与闭包模型面串字节相等）；未置位 ⇒ 静态模板
   * 逐字节不变（人侧糖腿 / ws 直连 ir 腿）。静态模板不可作 LLM 面默认腿出货
   * （sandbox=off/无 hook/无快照——承重论证见 P1-3 方案第 7 条）。
   */
  private def handler(tool: Tool, sr: Option[AgentRuntimePort], lib: Option[AgentLibraryView]): DevHandler =
    (args, callCtx) =>
      llmExecLocal.get.flatMap {
        case Some(exec) =>
          exec.run().map { r =>
            if r.isError then Left(IrError.commandFailed(r.content)): Either[IrError, StreamValue]
            else Right(StreamValue.Text(r.content)): Either[IrError, StreamValue]
          }
        case None =>
          val ctx = ToolContext(
            projectRoot = "",
            sharedResources = sr,
            actorSystem = sr.map(_.actorSystem),
            taskStore = Some(FileTaskStore),
            agentLibrary = lib
          ).copy(sessionId = Some(callCtx.sessionId), requestId = Some(callCtx.requestId))
          tool
            .call(args, ctx)
            .map {
              case Right(out) => Right(StreamValue.Text(out)): Either[IrError, StreamValue]
              case Left(err) => Left(IrError.commandFailed(err.message)): Either[IrError, StreamValue]
            }
            .handleErrorWith(e =>
              IO.pure(Left(IrError.commandFailed(s"${tool.name}: ${Option(e.getMessage).getOrElse(e.toString)}")))
            )
      }

end IrToolBridge
