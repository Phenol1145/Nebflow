package nebflow.ir

import cats.effect.{IO, Ref}
import io.circe.JsonObject
import io.circe.syntax.*
import munit.CatsEffectSuite

/**
 * 策略与审批的一致性用例（标准 §8）：C9/C10/C11/C13/C16/C23/C33/C35/C37/C38。
 *
 * `Ask` 的验收面是**双条件**：`status=await_approval` **且**副作用计数 = 0；`Deny` 则是
 * `status=rejected` + exit 125 + 计数 = 0。计数用 [[IrTestKit.probe]] 的真副作用读。
 */
class IrPolicySpec extends CatsEffectSuite:

  /** 声明 `FsRead(<prefix>)` 且带 `path` 路径参数的命令（cap 前缀判定的载体）。 */
  private def guarded(counter: Ref[IO, Int], prefix: String, name: String = "dev:test:guarded"): CommandDef =
    CommandDef(
      name = name,
      description = "fs-read guarded probe (test only)",
      argsSchema = DevCommands.fsSchema,
      binding = Binding.Dev((_, _) => counter.updateAndGet(_ + 1).map(n => Right(StreamValue.Text(s"read#$n")))),
      io = CommandIo(None, StreamKind.Text),
      pathArgs = Set("path"),
      caps = Set(Cap.FsRead(prefix)),
      audiences = Set(Audience.Human)
    )

  private def run(
    h: IrTestKit.Harness,
    plan: Ir,
    safety: Safety = Safety.AutoEdits,
    approval: Option[ApprovalCredential] = None,
    tenant: Tenant = Tenant.Human("local")
  ): IO[PlanResult] =
    h.router.submit(IrTestKit.req(plan, tenant, approval = approval), VfsRoot(h.root), safety)

  // ── C9 策略三态 ───────────────────────────────────────────

  test("C9 三态：Allow 执行；Ask ⇒ await_approval 且零执行；Deny ⇒ exit 125 且零执行"):
    IrTestKit.vfs().flatMap { root =>
      val allowRules = List(PolicyRule("dev:test:probe", Decision.Allow))
      val denyRules = List(PolicyRule("dev:test:probe", Decision.Deny("blocked by test rule", "test:deny")))
      for
        hAllow <- IrTestKit.harness(root, rules = allowRules)
        hAsk <- IrTestKit.harness(root)
        hDeny <- IrTestKit.harness(root, rules = denyRules)
        rAllow <- run(hAllow, IrTestKit.call("dev:test:probe"))
        cAllow <- hAllow.counter.get
        rAsk <- run(hAsk, IrTestKit.call("dev:test:probe"))
        cAsk <- hAsk.counter.get
        rDeny <- run(hDeny, IrTestKit.call("dev:test:probe"))
        cDeny <- hDeny.counter.get
      yield
        assertEquals(rAllow.status, Status.Done)
        assertEquals(cAllow, 1)
        assertEquals(rAsk.status, Status.AwaitApproval)
        assertEquals(rAsk.exit, None) // await_approval 的 exit 必须为 null
        assertEquals(rAsk.approval.isDefined, true)
        assertEquals(cAsk, 0)
        assertEquals(rDeny.status, Status.Rejected)
        assertEquals(rDeny.exit, Some(125))
        assertEquals(rDeny.error.map(_.code), Some(Codes.PolicyDenied))
        assertEquals(rDeny.error.flatMap(_.details("rule").flatMap(_.asString)), Some("test:deny"))
        assertEquals(cDeny, 0)
      end for
    }

  test("C10 先全判后执行：计划 = [Allow 副作用节点, Deny 节点] ⇒ 零执行，exit 125"):
    IrTestKit.vfs().flatMap { root =>
      val rules = List(
        PolicyRule("dev:test:probe", Decision.Allow),
        PolicyRule("dev:fs:*", Decision.Deny("blocked", "test:deny"))
      )
      IrTestKit.harness(root, rules = rules).flatMap { h =>
        val plan = Ir.Sequence(List(IrTestKit.call("dev:test:probe"), IrTestKit.call("dev:fs:ls")))
        run(h, plan).flatMap { r =>
          h.counter.get.map { count =>
            assertEquals(r.status, Status.Rejected)
            assertEquals(r.exit, Some(125))
            assertEquals(r.results, Nil)
            assertEquals(count, 0) // 整计划零执行
          }
        }
      }
    }

  test("C13 未知 cap ⇒ Deny（fail-closed，升级策略表才可放行）"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root, knownCapKinds = Set("FsRead")).flatMap { h =>
        run(h, IrTestKit.call("dev:test:probe")).flatMap { r =>
          h.counter.get.map { count =>
            assertEquals(r.status, Status.Rejected)
            assertEquals(r.error.flatMap(_.details("rule").flatMap(_.asString)), Some("fail-closed:unknown-cap"))
            assertEquals(count, 0)
          }
        }
      }
    }

  // ── C23/C16 cap 前缀判定 ─────────────────────────────────

  test("C23 cap 前缀拒绝：声明 FsRead(\"work\") 而 args.path=\"secret/x\" ⇒ 执行前 Deny（两级判定第二级）"):
    IrTestKit.vfs("secret/x" -> "s", "work/a" -> "a").flatMap { root =>
      Ref.of[IO, Int](0).flatMap { counter =>
        val r = new CommandRegistry()
        r.register(guarded(counter, "work"))
        IrTestKit.harness(root, registry = Some(r)).flatMap { h =>
          run(h, IrTestKit.call("dev:test:guarded", Some("secret/x"))).flatMap { res =>
            counter.get.map { c =>
              assertEquals(res.status, Status.Rejected)
              assertEquals(res.exit, Some(125))
              assertEquals(res.error.flatMap(_.details("rule").flatMap(_.asString)), Some("cap:prefix"))
              assertEquals(c, 0) // 执行前就拦住，副作用为零
            }
          }
        }
      }
    }

  test("C16 cap 判定在 canonical 后：a/../b 与 b 同判（`..` 折叠不许成为绕过）"):
    IrTestKit.vfs("work/b" -> "b").flatMap { root =>
      Ref.of[IO, Int](0).flatMap { counter =>
        val r = new CommandRegistry()
        r.register(guarded(counter, "work"))
        IrTestKit.harness(root, registry = Some(r)).flatMap { h =>
          for
            ok <- run(h, IrTestKit.call("dev:test:guarded", Some("work/a/../b")))
            denied <- run(h, IrTestKit.call("dev:test:guarded", Some("work/../secret/x")))
          yield
            assertEquals(ok.status, Status.Done) // 折叠后仍在 work/ 内
            assertEquals(denied.status, Status.Rejected) // 折叠后越出前缀
        }
      }
    }

  test("C16/cap×重解析点：work/link -> ../secret 时按真实落点判 cap（词法在 work/ 内也拒）"):
    IrTestKit.vfs("secret/x" -> "s").flatMap { root =>
      os.write(root / "work" / "keep", "k", createFolders = true)
      val link = root / "work" / "link"
      val created =
        try
          os.symlink(link, root / "secret" / "x")
          true
        catch
          case _: Exception =>
            // Windows 无特权退回目录 junction（不需要管理员，逃逸面等价）
            try
              os.proc("cmd", "/c", "mklink", "/J", link.toString, (root / "secret").toString).call(check = false)
              os.exists(link)
            catch case _: Exception => false
      assume(created, "no symlink/junction privilege on this host")
      Ref.of[IO, Int](0).flatMap { counter =>
        val r = new CommandRegistry()
        r.register(guarded(counter, "work"))
        IrTestKit.harness(root, registry = Some(r)).flatMap { h =>
          for
            linked <- run(h, IrTestKit.call("dev:test:guarded", Some("work/link/x")))
            count <- counter.get
          yield
            assertEquals(linked.status, Status.Rejected)
            assertEquals(linked.error.flatMap(_.details("rule").flatMap(_.asString)), Some("cap:prefix"))
            assertEquals(count, 0) // 真实落点在 work/ 之外 ⇒ 执行前拒绝
        }
      }
    }

  test("C14/C15 越根与 `~` 由**计划校验**拦（exit 2，不是策略拒绝）"):
    IrTestKit.vfs().flatMap { root =>
      Ref.of[IO, Int](0).flatMap { counter =>
        val r = new CommandRegistry()
        r.register(guarded(counter, "."))
        IrTestKit.harness(root, registry = Some(r)).flatMap { h =>
          for
            escape <- run(h, IrTestKit.call("dev:test:guarded", Some("../../etc/passwd")))
            tilde <- run(h, IrTestKit.call("dev:test:guarded", Some("~/x")))
            count <- counter.get
          yield
            assertEquals(escape.exit, Some(2))
            assertEquals(escape.error.map(_.code), Some(Codes.SchemaBadValue))
            assertEquals(tilde.error.map(_.code), Some(Codes.SchemaBadValue))
            assertEquals(count, 0)
        }
      }
    }

  // ── C11/C35/C37/C38 审批闭环 ─────────────────────────────

  test("C37 审批闭环：Ask → 展示 → 带有效凭据重提交 ⇒ 折算 Allow 并执行（不再次 Ask）"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val plan = IrTestKit.call("dev:test:probe")
        for
          asked <- run(h, plan)
          _ = assertEquals(asked.status, Status.AwaitApproval)
          cred = IrTestKit.approve(asked)
          done <- run(h, plan, approval = Some(cred))
          count <- h.counter.get
        yield
          assertEquals(done.status, Status.Done)
          assertEquals(done.exit, Some(0))
          assertEquals(count, 1)
      }
    }

  test("C11 审批绑定：用 digest A 的凭据执行 digest B 的计划 ⇒ policy.denied，零执行"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val planA = IrTestKit.call("dev:test:probe")
        val planB = Ir.Sequence(List(IrTestKit.call("dev:test:probe"), IrTestKit.call("dev:fs:ls")))
        for
          asked <- run(h, planA)
          cred = IrTestKit.approve(asked)
          stolen <- run(h, planB, approval = Some(cred))
          count <- h.counter.get
        yield
          assertEquals(stolen.status, Status.Rejected)
          assertEquals(stolen.exit, Some(125))
          assertEquals(stolen.error.flatMap(_.details("rule").flatMap(_.asString)), Some("approval:planDigest"))
          assertEquals(count, 0)
      }
    }

  test("C35 审批单次使用：同一 nonce 重放第二次 ⇒ rejected + policy.denied"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val plan = IrTestKit.call("dev:test:probe")
        for
          asked <- run(h, plan)
          cred = IrTestKit.approve(asked)
          first <- run(h, plan, approval = Some(cred))
          second <- run(h, plan, approval = Some(cred))
          count <- h.counter.get
        yield
          assertEquals(first.status, Status.Done)
          assertEquals(second.status, Status.Rejected)
          assertEquals(second.error.flatMap(_.details("rule").flatMap(_.asString)), Some("approval:nonce"))
          assertEquals(count, 1) // 只执行过一次
      }
    }

  test("C38 凭据失效（TOCTOU）：审批后热换描述符（caps 变）再重提交 ⇒ capsDigest 不匹配 ⇒ 重新 await_approval"):
    IrTestKit.vfs().flatMap { root =>
      Ref.of[IO, Int](0).flatMap { counter =>
        val before = new CommandRegistry()
        before.register(IrTestKit.probe(counter)) // FsWrite(".")
        IrTestKit.harness(root, registry = Some(before)).flatMap { h1 =>
          val plan = IrTestKit.call("dev:test:probe")
          for
            asked <- run(h1, plan)
            cred = IrTestKit.approve(asked)
            // 热注册：同名命令的 caps 提升（读 → 读+写）
            after = new CommandRegistry()
            _ = after.register(IrTestKit.probe(counter).copy(caps = Set(Cap.FsRead("."), Cap.FsWrite("."))))
            h2 <- IrTestKit.harness(root, registry = Some(after))
            again <- run(h2, plan, approval = Some(cred))
            count <- counter.get
          yield
            assertEquals(again.status, Status.AwaitApproval)
            assertEquals(again.approval.map(_.capsDigest) == Some(cred.capsDigest), false)
            assertEquals(count, 0)
        }
      }
    }

  test("审批凭据的 rest：过期 ⇒ rejected；租户不符 ⇒ rejected"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val plan = IrTestKit.call("dev:test:probe")
        for
          asked <- run(h, plan)
          expired = IrTestKit.approve(asked).copy(expiresAt = "2000-01-01T00:00:00Z")
          otherTenant = IrTestKit.approve(asked).copy(approvedBy = "human:someone-else")
          rExpired <- run(h, plan, approval = Some(expired))
          rTenant <- run(h, plan, approval = Some(otherTenant), tenant = Tenant.Human("local"))
          count <- h.counter.get
        yield
          assertEquals(rExpired.status, Status.Rejected)
          assertEquals(rExpired.error.flatMap(_.details("rule").flatMap(_.asString)), Some("approval:expired"))
          assertEquals(rTenant.status, Status.Rejected)
          assertEquals(rTenant.error.flatMap(_.details("rule").flatMap(_.asString)), Some("approval:tenant"))
          assertEquals(count, 0)
      }
    }

  // ── C33 注册期校验 ───────────────────────────────────────

  test("C33 同档碰撞 ⇒ 注册失败；跨档同名 ⇒ 高优先档覆盖（不报错）"):
    val r = new CommandRegistry()
    assertEquals(r.register(DevCommands.fsLs).isRight, true)
    val again = r.register(DevCommands.fsLs)
    assertEquals(again.left.toOption.map(_.code), Some(Codes.SchemaNameInvalid))
    assertEquals(again.left.toOption.flatMap(_.details("reason").flatMap(_.asString)), Some("collision"))

    val cross = new CommandRegistry()
    val userTier = DevCommands.fsLs.copy(trust = Trust.User)
    val builtin = DevCommands.fsLs.copy(trust = Trust.Builtin)
    assertEquals(cross.register(userTier).map(_.replaced), Right(false))
    assertEquals(cross.register(builtin).map(_.replaced), Right(true)) // Higher tier wins
    assertEquals(cross.get("dev:fs:ls").map(_.trust), Some(Trust.Builtin))
    // 反向顺序：低档不覆盖高档
    val cross2 = new CommandRegistry()
    assertEquals(cross2.register(builtin).map(_.replaced), Right(false))
    assertEquals(cross2.register(userTier).map(_.replaced), Right(false))
    assertEquals(cross2.get("dev:fs:ls").map(_.trust), Some(Trust.Builtin))

  test("C33 Project 档受工作区信任门：未授信 ⇒ 不注册（policy.untrusted）"):
    val untrusted = new CommandRegistry(workspaceTrusted = false)
    val proj = DevCommands.fsLs.copy(trust = Trust.Project)
    assertEquals(untrusted.register(proj).left.toOption.map(_.code), Some(Codes.PolicyUntrusted))
    assertEquals(untrusted.get("dev:fs:ls"), None)
    val trusted = new CommandRegistry(workspaceTrusted = true)
    assertEquals(trusted.register(proj).map(_.name), Right("dev:fs:ls"))

  test("§7.2/§7.3 命名：命名空间必须与 binding 一致；外部标识做确定性规范化"):
    val r = new CommandRegistry()
    // §7.2：name 首段必须与 binding 类别一致（`bash:` 名配 `Dev` 绑定 ⇒ 注册失败）
    assertEquals(
      r.register(DevCommands.fsLs.copy(name = "bash:fs:ls")).left.toOption.map(_.code),
      Some(Codes.SchemaNameInvalid)
    )
    assertEquals(
      r.register(DevCommands.fsLs.copy(name = "mcp:fs:ls")).left.toOption.map(_.code),
      Some(Codes.SchemaNameInvalid)
    )
    val mcp = CommandDef(
      name = "mcp:GitHub:Create_Issue",
      description = "mcp (test only)",
      argsSchema = JsonObject.empty,
      binding = Binding.Mcp("GitHub", "Create_Issue"),
      io = CommandIo(None, StreamKind.Text),
      audiences = Set(Audience.Human)
    )
    assertEquals(r.register(mcp).map(_.name), Right("mcp:github:create_issue"))
    assertEquals(r.get("mcp:github:create_issue").isDefined, true)
    assertEquals(r.get("mcp:GitHub:Create_Issue"), None) // N2：禁止大小写不敏感匹配

  test("§7.5 llmName：LLM 可见必须声明且全局唯一"):
    val r = new CommandRegistry()
    val llmVisible = DevCommands.fsLs.copy(name = "dev:fs:ls", audiences = Set(Audience.Llm))
    assertEquals(
      r.register(llmVisible).left.toOption.flatMap(_.details("reason").flatMap(_.asString)),
      Some("llm_name_missing")
    )
    assertEquals(r.register(llmVisible.copy(llmName = Some("Read"))).map(_.name), Right("dev:fs:ls"))
    // 唯一性：同名 llmName 的第二条命令必须被拒（Llm 可见才要求声明，故这里也要带 audiences）
    val dup = DevCommands.fsCat.copy(audiences = Set(Audience.Llm), llmName = Some("Read"))
    assertEquals(
      r.register(dup).left.toOption.flatMap(_.details("reason").flatMap(_.asString)),
      Some("llm_name_duplicate")
    )

end IrPolicySpec
