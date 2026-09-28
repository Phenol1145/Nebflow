package nebflow.ir

import io.circe.syntax.*
import io.circe.{Json, JsonObject}

/** 流类型（§5.1/§5.3）：v1 只有 `jsonl` / `text`（无 `binary`，[D3]）。 */
enum StreamKind:
  case Jsonl, Text

object StreamKind:

  def wire(k: StreamKind): String = k match
    case Jsonl => "jsonl"
    case Text => "text"
end StreamKind

/**
 * 管道契约（§5.3）。**声明的是能力，不是必然行为**（[D19]）：`stdin = None` 表示
 * 「不接受 stdin」（`none`）；声明只用于**管道类型匹配**，**禁止**被当作「必然消费/
 * 必然产出」来校验（`dev:fs:cat` 有 path 读文件、无 path 读 stdin 正是这条的由来）。
 */
final case class CommandIo(stdin: Option[StreamKind], stdout: StreamKind, outSchema: Option[JsonObject] = None)

/** 能力词表（§8.1）。**声明替代静态分析**——权限输入是描述符里的 caps，不是从文本推断。 */
enum Cap:
  case FsRead(prefix: String)
  case FsWrite(prefix: String)
  case Net(host: String)
  case Exec
  case Secret(name: String)
  case MemoryWrite

object Cap:

  /** 词表键（策略引擎按此判「认识/不认识」：不认识 ⇒ Deny，fail-closed，§8.1）。 */
  def kind(c: Cap): String = c match
    case _: Cap.FsRead => "FsRead"
    case _: Cap.FsWrite => "FsWrite"
    case _: Cap.Net => "Net"
    case Cap.Exec => "Exec"
    case _: Cap.Secret => "Secret"
    case Cap.MemoryWrite => "MemoryWrite"

  /** 审计/审批展示形态。 */
  def render(c: Cap): String = c match
    case Cap.FsRead(p) => s"FsRead($p)"
    case Cap.FsWrite(p) => s"FsWrite($p)"
    case Cap.Net(h) => s"Net($h)"
    case Cap.Exec => "Exec"
    case Cap.Secret(n) => s"Secret($n)"
    case Cap.MemoryWrite => "MemoryWrite"

end Cap

/** 信任档（§7.4）：`Project` 未过工作区信任门 ⇒ **不注册**（记审计 `router.trust.rejected`）。 */
enum Trust:
  case Builtin, User, Project

object Trust:

  /** 覆盖优先级：`Builtin` 恒赢 > `Project`(team/flow 层) > `User`(global 层)。 */
  def priority(t: Trust): Int = t match
    case Builtin => 3
    case Project => 2
    case User => 1
end Trust

/** 结果去向（复用 V2 术语）：看板卡｜注入会话｜两者。 */
enum Delivery:
  case Panel, Inject, Both

/** 人侧 argv 糖的形参（§7.1；先例 `nebflow/cli/CliModel.scala:42 CliParam`）。 */
final case class Param(
  name: String,
  positional: Option[Int] = None,
  required: Boolean = false,
  default: Option[Json] = None,
  isFlag: Boolean = false,
  description: String = ""
)

/** 进程内命令实现（`dev:`）：`Left` ⇒ exit 1 + `command.failed`（§12 适配口径，同 `Tool` 的二元 `Either`）。 */
type DevHandler = (JsonObject, CallCtx) => cats.effect.IO[Either[IrError, StreamValue]]

/**
 * 实现机制（§7.2）。命名空间表示**实现来源**，**不表示风险**（[D10]）：风险一律由
 * [[Cap]] 表达（`http` 客户端实现仍在 `dev:`，其网络访问靠 `Net` cap 约束，[D11]）。
 */
enum Binding:
  case Dev(handler: DevHandler)
  case Mcp(server: String, tool: String)
  case Node(extension: String, entry: String)
  case RealBash(template: String)
  case Http(method: String, url: String)

object Binding:

  /** 首段（§7.2：`name` 的首段与 binding 类别**必须**一致，否则注册失败）。 */
  def namespace(b: Binding): String = b match
    case _: Dev => "dev"
    case _: Mcp => "mcp"
    case _: Node => "ext"
    case _: RealBash => "bash"
    case _: Http => "dev" // [D11]：Http 归 dev:，不设 http: 命名空间
end Binding

/**
 * 命令描述符 = **能力单一事实源**（§7.1）。注册一次即三面可见（人补全表 / LLM 工具表 /
 * 脚本命名空间）——可见性以 [[audiences]] 为准（§7.4：未列出的面**禁止**调用）。
 *
 * `llmName`（§7.5）：模型面名字稳定性的载体。**禁止**由 IR 名字符串推导（推导会在改名时
 * 悄悄改提示词）；迁移期 = 现有工具名原样保留。
 */
final case class CommandDef(
  name: String,
  description: String,
  argsSchema: JsonObject,
  binding: Binding,
  io: CommandIo,
  pathArgs: Set[String] = Set.empty,
  caps: Set[Cap] = Set.empty,
  params: List[Param] = Nil,
  delivery: Delivery = Delivery.Panel,
  trust: Trust = Trust.Builtin,
  audiences: Set[Audience] = Set(Audience.Human),
  llmName: Option[String] = None,
  deprecated: Option[String] = None
):

  def hasSecretCap: Boolean = caps.exists {
    case _: Cap.Secret => true
    case _ => false
  }

  def capKinds: Set[String] = caps.map(Cap.kind)
end CommandDef

/**
 * 计划里一个**需要判定与执行**的节点：`Call`（真实命令）或 `Redirect`（伪命令 `@redirect:*`，
 * cap 由 target 计算 —— §8.1/§5.5 S4）。判定是**逐节点、带 args 的**（§8.1 末：计划 cap
 * 并集只用于审批展示与审计，**禁止**作为判定输入）。
 */
final case class NodeTarget(
  node: String,
  command: String,
  caps: Set[Cap],
  args: JsonObject,
  pathArgs: Map[String, VfsPath.Canon] = Map.empty,
  binding: Option[Binding] = None
):
  def capNames: List[String] = caps.map(Cap.render).toList.sorted

  def capKinds: Set[String] = caps.map(Cap.kind)
end NodeTarget

object NodeTarget:
  /** Redirect 伪命令名（IR 命名语法非法 ⇒ 与真实命令名不可能碰撞）。 */
  def redirectName(op: RedirectOp): String = s"@redirect:${RedirectOp.wire(op)}"
end NodeTarget
