package nebflow.ir

import io.circe.syntax.*

/**
 * 命令注册表 = 能力单一事实源（标准 §7、§7.4）。
 *
 * 三件事在这里发生（都在**注册期**，不在执行期）：
 *  - 名字语法与命名空间一致性（§7.2/§7.3 N1–N4）——外部标识**规范化**且碰撞即拒；
 *  - 形参与 schema 一致性（§7.1：糖不许造出 schema 之外的参数）；
 *  - 信任档与作用域（§7.4/[D29]）：`Project` 档命令**必须**在工作区作用域视图内，
 *    未授信 ⇒ **不注册**（三面一次性不可见）。
 *
 * 同名语义（§7.4）：**同一档内**碰撞 ⇒ 注册失败；**跨档**碰撞 ⇒ 高优先档覆盖
 * （不报错，但有审计 —— 调用方按 [[Registration.replaced]] 落账）。
 *
 * `workspaceTrusted` 就是 [D29] 的「作用域视图」：进程级表只应装载 `Builtin`/`User`；
 * 项目档装载发生在按工作区构造的视图上（装配面职责）。
 */
final class CommandRegistry(workspaceTrusted: Boolean = true):

  private val table = scala.collection.mutable.LinkedHashMap.empty[String, CommandDef]

  /** 注册结果：canonical IR 名 + 是否覆盖了既有条目（跨档高优先覆盖时 true）。 */
  final case class Registration(name: String, replaced: Boolean)

  /**
   * `raw` → canonical IR 名（N3/N4）。外部面（`mcp:`/`ext:`）**规范化**；`dev:`/`bash:`
   * 是自撰名字，非法即拒（不静默改写自撰名）。
   */
  def canonicalName(raw: String, binding: Binding): Either[IrError, String] =
    val ns = Binding.namespace(binding)
    val head = raw.split(":", -1).headOption.getOrElse("")
    if head != ns then
      Left(IrError.nameInvalid(raw, s"namespace '$head' does not match binding '$ns' (name must start with '$ns:')"))
    else
      binding match
        case _: Binding.Mcp | _: Binding.Node =>
          val normalized = Names.normalize(raw)
          Names.syntax(normalized).map(_ => normalized)
        case _ => Names.syntax(raw).map(_ => raw)
  end canonicalName

  def register(cmd: CommandDef): Either[IrError, Registration] =
    for
      canonical <- canonicalName(cmd.name, cmd.binding)
      _ <- validateCaps(cmd)
      _ <- validateParams(cmd)
      _ <- validateLlmName(cmd)
      _ <- trustGate(cmd)
      reg <- install(canonical, cmd)
    yield reg

  def get(name: String): Option[CommandDef] = table.get(name)

  def names: List[String] = table.keys.toList

  def all: List[CommandDef] = table.values.toList

  def size: Int = table.size

  // ── 安装（同名语义 §7.4） ──────────────────────────────────

  private def install(canonical: String, cmd: CommandDef): Either[IrError, Registration] =
    table.get(canonical) match
      case None =>
        table.update(canonical, cmd)
        Right(Registration(canonical, replaced = false))
      case Some(existing) =>
        val incoming = Trust.priority(cmd.trust)
        val current = Trust.priority(existing.trust)
        if incoming == current then
          Left(
            IrError
              .nameInvalid(canonical, "already registered in the same trust tier (N4 collision)")
              .withDetail("reason", "collision".asJson)
          )
        else if incoming > current then
          table.update(canonical, cmd)
          Right(Registration(canonical, replaced = true))
        else
          // 低档不覆盖高档（"高层覆盖低层、builtin 恒赢"沿用现状，§7.4）
          Right(Registration(canonical, replaced = false))

  private def validateCaps(cmd: CommandDef): Either[IrError, Unit] =
    cmd.caps.collectFirst {
      case Cap.FsRead(p) if VfsPath.canonPrefix(p).isLeft => s"FsRead prefix '$p'"
      case Cap.FsWrite(p) if VfsPath.canonPrefix(p).isLeft => s"FsWrite prefix '$p'"
    } match
      case Some(msg) => Left(IrError.badValue(s"cap prefix must be a VFS path: $msg ([P1]/[P3]/[P4])"))
      case None => Right(())

  private def validateParams(cmd: CommandDef): Either[IrError, Unit] =
    val declared = cmd.argsSchema("properties").flatMap(_.asObject).map(_.keys.toSet).getOrElse(Set.empty)
    val positional = cmd.params.flatMap(_.positional)
    // ① §9:543 红线 4：`mcp:`/`ext:` 的 params 恒为空（v1 不为 MCP 造 argv 糖，外部面只有 --json）
    val foreignEmpty: Either[IrError, Unit] =
      if (cmd.binding.isInstanceOf[Binding.Mcp] || cmd.binding.isInstanceOf[Binding.Node]) && cmd.params.nonEmpty then
        Left(
          IrError
            .invalidArgs(
              s"command '${cmd.name}' is ${Binding.namespace(cmd.binding)}-bound: params must stay empty " +
                "(§9 red line 4 — the foreign face is --json only)"
            )
            .withDetail("reason", "params_foreign_empty".asJson)
            .withDetail("namespace", Binding.namespace(cmd.binding).asJson)
        )
      else Right(())
    // ② positional 槽位排序后必须 == 0..n-1（§9「按 Param.positional 升序绑定，不得跳号」前移到注册期）
    val noGaps: Either[IrError, Unit] =
      if positional.sorted != (0 until positional.length).toList then
        Left(
          IrError
            .invalidArgs("positional slots must be 0..n-1 without gaps or duplicates (§9 binds by ascending index)")
            .withDetail("reason", "positional_gap".asJson)
            .withDetail("slots", positional.sorted.asJson)
        )
      else Right(())
    cmd.params.find(p => !declared.contains(p.name)) match
      case Some(p) =>
        Left(
          IrError
            .invalidArgs(s"param '${p.name}' is not declared in argsSchema.properties (糖不许造出 schema 之外的参数)")
            .withDetail("param", p.name.asJson)
        )
      case None =>
        for
          _ <- foreignEmpty
          _ <-
            // 重复槽位是 ② 的特例，先以独立报错面钉住（同站点双守卫）
            if positional.distinct.length != positional.length then
              Left(IrError.invalidArgs("duplicate positional index in params"))
            else Right(())
          _ <- noGaps
        yield ()

    end match

  end validateParams

  /** §7.5：`Audience.Llm ∈ audiences ⇒ llmName 必须声明`，且**必须**全局唯一。 */
  private def validateLlmName(cmd: CommandDef): Either[IrError, Unit] =
    if !cmd.audiences.contains(Audience.Llm) then Right(())
    else
      cmd.llmName match
        case None =>
          Left(
            IrError
              .invalidArgs(s"command '${cmd.name}' is LLM-visible but declares no llmName (§7.5)")
              .withDetail("reason", "llm_name_missing".asJson)
          )
        case Some(name) =>
          table.collectFirst { case (k, d) if d.llmName.contains(name) && k != cmd.name => k } match
            case Some(other) =>
              Left(
                IrError
                  .invalidArgs(s"llmName '$name' is already used by '$other' (§7.5 uniqueness)")
                  .withDetail("reason", "llm_name_duplicate".asJson)
              )
            case None => Right(())

  /** §7.4：`Project` 未过工作区信任门 ⇒ 不注册（调用方记审计 `router.trust.rejected`）。 */
  private def trustGate(cmd: CommandDef): Either[IrError, Unit] =
    if cmd.trust == Trust.Project && !workspaceTrusted then Left(IrError.policyUntrusted(cmd.name))
    else Right(())

end CommandRegistry
