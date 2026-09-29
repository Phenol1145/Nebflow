package nebflow.ir

import cats.effect.IO
import io.circe.JsonObject
import io.circe.syntax.*
import munit.CatsEffectSuite

/**
 * 执行语义的一致性用例（标准 §5/§6）：C1/C2/C3/C7/C8/C12/C17/C18/C19/C29/C30/C31/C34
 * 与 C21/C25（可见面两处检查点）。
 *
 * 执行面用 `AutoAll` 档跑（应用启动默认档）：`Ask` 折算 `Allow` 而 `Deny` 不动 —— 三态
 * 用例在 `IrPolicySpec` 里用显式档位覆盖。
 */
class IrExecSpec extends CatsEffectSuite:

  private def run(
    h: IrTestKit.Harness,
    plan: Ir,
    safety: Safety = Safety.AutoAll,
    tenant: Tenant = Tenant.Human("local"),
    ingress: Ingress = Ingress.Human,
    approval: Option[ApprovalCredential] = None
  ): IO[PlanResult] =
    h.router.submit(IrTestKit.req(plan, tenant, ingress, approval), VfsRoot(h.root), safety)

  private def file(root: os.Path, rel: String): String = os.read(root / os.RelPath(rel))

  // ── C1/C2/C8：单命令、管道、唯一强转 ──────────────────────

  test("C1 单命令（程序构造）：dev:fs:ls → exit 0 + JSONL，按 name 字典序"):
    IrTestKit.vfs("b.txt" -> "b", "a.txt" -> "a", "sub/c.txt" -> "c").flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        run(h, IrTestKit.call("dev:fs:ls")).map { r =>
          assertEquals(r.status, Status.Done)
          assertEquals(r.exit, Some(0))
          val rows = r.results.head.stdout.get.asInstanceOf[StreamValue.Jsonl].items
          assertEquals(
            rows.flatMap(_.hcursor.downField("name").as[String].toOption),
            List("a.txt", "b.txt", "sub")
          )
          val sub = rows.find(_.hcursor.downField("name").as[String].toOption.contains("sub")).get
          assertEquals(sub.hcursor.downField("kind").as[String].toOption, Some("dir"))
          assertEquals(sub.hcursor.downField("size").focus, None) // size 仅文件出现
        }
      }
    }

  test("C2/C8 管道 ls | cat（jsonl→text 是唯一允许的强转）"):
    IrTestKit.vfs("a.txt" -> "a").flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        run(h, Ir.Pipe(List(IrTestKit.call("dev:fs:ls"), IrTestKit.call("dev:fs:cat")))).map { r =>
          assertEquals(r.status, Status.Done)
          assertEquals(r.results.map(_.node), List("0.0", "0.1"))
          val text = r.results(1).stdout.get.asInstanceOf[StreamValue.Text].value
          assert(text.contains("\"name\":\"a.txt\""), text)
        }
      }
    }

  test("C3 未知命令 ⇒ exit 127 + router.unknown_command，零结果"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        run(h, IrTestKit.call("dev:fs:nope")).map { r =>
          assertEquals(r.status, Status.Invalid)
          assertEquals(r.exit, Some(127))
          assertEquals(r.error.map(_.code), Some(Codes.UnknownCommand))
          assertEquals(r.results, Nil)
        }
      }
    }

  test("C7 类型不匹配（text 生产端 → 无 stdin 消费端）⇒ 执行前拒绝 + router.stream.mismatch"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        run(h, Ir.Pipe(List(IrTestKit.call("dev:fs:cat"), IrTestKit.call("dev:fs:ls")))).map { r =>
          assertEquals(r.status, Status.Invalid)
          assertEquals(r.exit, Some(2))
          assertEquals(r.error.map(_.code), Some(Codes.StreamMismatch))
          assertEquals(r.results, Nil)
        }
      }
    }

  // ── C12 fail-fast ─────────────────────────────────────────

  test("C12 fail-fast：首级 exit 1 ⇒ 二级不执行，exit = 1，结果只含首级"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val pipe = Ir.Pipe(List(IrTestKit.call("dev:test:boom"), IrTestKit.call("dev:test:probe")))
        run(h, pipe).flatMap { r =>
          h.counter.get.map { count =>
            assertEquals(r.exit, Some(1))
            assertEquals(r.results.map(_.node), List("0.0"))
            assertEquals(count, 0) // 二级没跑
          }
        }
      }
    }

  // ── C17/C29 JSONL framing（消费端） ────────────────────────

  test("C17 J4：末行无换行 + 空白行都容忍"):
    IrTestKit.vfs("in.jsonl" -> "{\"a\":1}\n\n{\"b\":2}").flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val plan = Ir.Redirect(RedirectOp.In, "in.jsonl", IrTestKit.call("dev:test:sink"))
        run(h, plan).map { r =>
          assertEquals(r.status, Status.Done)
          // [J4]：末行无换行 + 空白行都容忍 ⇒ 两条都进流（Redirect 自身 stdout 恒空，见 S7）
          assertEquals(r.results.head.stdout, Some(StreamValue.Text("got 2")))
          assertEquals(r.finalStdout, Some(StreamValue.Text("")))
        }
      }
    }

  test("C29 J2/J5：一行两值、跨行 pretty-print ⇒ router.stream.violation（禁止静默丢弃）"):
    IrTestKit.vfs("one.jsonl" -> "{\"a\":1}{\"b\":2}", "pretty.jsonl" -> "{\n  \"a\": 1\n}").flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val bad = Ir.Redirect(RedirectOp.In, "one.jsonl", IrTestKit.call("dev:test:sink"))
        val pretty = Ir.Redirect(RedirectOp.In, "pretty.jsonl", IrTestKit.call("dev:test:sink"))
        run(h, bad).flatMap { r1 =>
          run(h, pretty).map { r2 =>
            assertEquals(r1.error.map(_.code), Some(Codes.StreamViolation))
            assertEquals(r2.error.map(_.code), Some(Codes.StreamViolation))
          }
        }
      }
    }

  // ── C18 限额：失败不截断 ─────────────────────────────────

  test("C18 超 router.limit.output ⇒ 失败（不是截断），细节含已产字节数"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit
        .harness(root, limits = IrLimits(maxTextBytes = 8), extra = List(IrTestKit.emitter("0123456789")))
        .flatMap { h =>
          run(h, IrTestKit.call("dev:test:emit")).map { r =>
            assertEquals(r.exit, Some(1))
            assertEquals(r.error.map(_.code), Some(Codes.LimitOutput))
            assertEquals(r.error.flatMap(_.details("produced")).flatMap(_.asNumber).flatMap(_.toInt), Some(10))
            assertEquals(r.finalStdout, None) // 截断不存在
          }
        }
    }

  test("C30 J6 声明与实况不符 ⇒ router.stream.violation"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit
        .harness(root, extra = List(IrTestKit.emitter("not jsonl", declared = StreamKind.Jsonl)))
        .flatMap { h =>
          run(h, IrTestKit.call("dev:test:emit")).map { r =>
            assertEquals(r.exit, Some(1))
            assertEquals(r.error.map(_.code), Some(Codes.StreamViolation))
          }
        }
    }

  // ── C31 Redirect 先判后写 ────────────────────────────────

  test("C31 先判后写：inner 失败 ⇒ 目标文件不被创建/截断（S2 偏离 bash）"):
    IrTestKit.vfs("out.txt" -> "keep").flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        run(h, Ir.Redirect(RedirectOp.Out, "out.txt", IrTestKit.call("dev:test:boom"))).flatMap { r =>
          IO.blocking {
            assertEquals(r.exit, Some(1))
            assertEquals(file(root, "out.txt"), "keep") // 未被截断
          }
        }
      }
    }

  test("S1/S7 Redirect(out) 成功路径：捕获 inner stdout 落盘，自身 stdout 恒空 text"):
    IrTestKit.vfs("a.txt" -> "hello").flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val plan = Ir.Redirect(RedirectOp.Out, "copy.txt", IrTestKit.call("dev:fs:cat", Some("a.txt")))
        run(h, plan).map { r =>
          assertEquals(r.status, Status.Done)
          assertEquals(file(root, "copy.txt"), "hello")
          assertEquals(r.results.last.stdout, Some(StreamValue.Text("")))
        }
      }
    }

  test("S5 父目录不存在 ⇒ exit 1 + command.failed（禁止隐式创建父目录）"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val plan = Ir.Redirect(RedirectOp.Out, "nope/out.txt", IrTestKit.call("dev:fs:ls"))
        run(h, plan).map { r =>
          assertEquals(r.exit, Some(1))
          assertEquals(r.error.map(_.code), Some(Codes.CommandFailed))
          assertEquals(os.exists(root / "nope"), false)
        }
      }
    }

  // ── C19 审计覆盖 ─────────────────────────────────────────

  test("C19 审计覆盖：执行与拒绝都每节点一条，且不含 args 原文/凭证值"):
    IrTestKit.vfs("secret-name.txt" -> "s").flatMap { root =>
      IrTestKit.harness(root, rules = List(PolicyRule("dev:test:probe", Decision.Deny("nope", "test:deny")))).flatMap {
        h =>
          // 允许的计划：两个节点都执行 ⇒ 两条记录，带 exit
          run(h, Ir.Pipe(List(IrTestKit.call("dev:fs:ls"), IrTestKit.call("dev:fs:cat")))).flatMap { ok =>
            assertEquals(ok.status, Status.Done)
            h.records.flatMap { recs1 =>
              // 拒绝的计划：命中 Deny ⇒ 记录在场、无 exit（零执行）
              run(h, Ir.Sequence(List(IrTestKit.call("dev:test:probe"), IrTestKit.call("dev:fs:ls")))).flatMap { bad =>
                assertEquals(bad.status, Status.Rejected)
                h.records.map { recs =>
                  assertEquals(recs1.map(_.node), Vector("0.0", "0.1"))
                  assertEquals(recs1.forall(_.exit.isDefined), true)
                  assertEquals(recs1.forall(_.decision == "allow"), true)
                  val denied = recs.drop(recs1.length)
                  // §8.4 判定短路：遇首个 Deny 即停判 ⇒ 审计/审批面只含**已被判定的节点**
                  // （0.0 判到 Deny 后不再判 0.1）——该取舍必须如实呈现，不假装整计划判过
                  assertEquals(denied.map(_.node), Vector("0.0"))
                  assertEquals(denied.head.decision, "deny")
                  assertEquals(denied.head.rule, Some("test:deny"))
                  assertEquals(denied.forall(_.exit.isEmpty), true)
                  // 无凭证值 / 无 args 原文：路径名只以 digest 形式入账
                  val rendered = recs.map(_.toJson.noSpaces).mkString
                  assertEquals(rendered.contains("secret-name.txt"), false)
                }
              }
            }
          }
      }
    }

  test("C19 审计字段：tenant/ingress/safety/ir/capsSource 齐备"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        run(h, IrTestKit.call("dev:fs:ls")).flatMap { _ =>
          h.records.map { recs =>
            val r = recs.head
            assertEquals(r.tenant, "human:local")
            assertEquals(r.ingress, "human")
            assertEquals(r.ir, Ir.Version)
            assertEquals(r.safety, "auto-all")
            assertEquals(r.capsSource, Codes.CapsDeclared)
            assertEquals(r.caps, List("FsRead(.)"))
            assertEquals(r.argsDigest.length, 64)
          }
        }
      }
    }

  // ── C21/C25 可见面：两处检查点都要拦 ──────────────────────

  test("C21/C25 检查点①（lowering）：LLM-only 命令从 human ingress 调 ⇒ 拦在计划期"):
    IrTestKit.vfs().flatMap { root =>
      val llmOnly = IrTestKit
        .probe(cats.effect.Ref.unsafe[IO, Int](0))
        .copy(name = "dev:test:llmonly", audiences = Set(Audience.Llm), caps = Set.empty, llmName = Some("TestLlmOnly"))
      IrTestKit.harness(root, extra = List(llmOnly)).flatMap { h =>
        run(h, IrTestKit.call("dev:test:llmonly")).map { r =>
          assertEquals(r.status, Status.Invalid)
          assertEquals(r.error.map(_.code), Some(Codes.InvalidArgs))
          assertEquals(r.error.flatMap(_.details("reason").flatMap(_.asString)), Some("audience"))
        }
      }
    }

  test("C25 检查点②（派发时，纵深防御）：手工计划绕过 lowering 也必须被拦"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val llmOnly = CommandDef(
          name = "dev:fs:ls",
          description = "llm-only copy of ls (test only)",
          argsSchema = DevCommands.fsSchema,
          binding = Binding.Dev((_, _) => IO.pure(Right(StreamValue.Text("leaked")))),
          io = CommandIo(None, StreamKind.Text),
          caps = Set.empty,
          audiences = Set(Audience.Llm)
        )
        val ir = IrTestKit.call("dev:fs:ls")
        val plan = Plan(
          ir,
          Vector(
            PlannedNode(
              "0",
              NodeTarget("0", "dev:fs:ls", Set.empty, JsonObject.empty, Map.empty, Some(llmOnly.binding)),
              Decision.Allow,
              Some(llmOnly),
              "digest"
            )
          ),
          "plan-digest",
          "caps-digest"
        )
        val ctx = CallCtx(Tenant.Human("local"), Ingress.Human, "s", "r", "0", VfsRoot(root), Safety.AutoAll)
        Executor.run(plan, ctx, IrLimits.default, h.policy).map { out =>
          val res = out.results.head
          assertEquals(res.exit, 2)
          assertEquals(res.stdout, None) // 未泄漏
          assertEquals(res.error.map(_.code), Some(Codes.InvalidArgs))
        }
      }
    }

  // ── C34 模型不可派发计划 ─────────────────────────────────

  test("C34 [L3]：llm ingress 提交含 Pipe 的计划 ⇒ 拒绝（模型只能走 tool_call 的一次 Call）"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val pipe = Ir.Pipe(List(IrTestKit.call("dev:fs:ls"), IrTestKit.call("dev:fs:cat")))
        run(h, pipe, tenant = Tenant.Llm("sess", "Nebula"), ingress = Ingress.Llm).map { r =>
          assertEquals(r.status, Status.Invalid)
          assertEquals(r.error.flatMap(_.details("reason").flatMap(_.asString)), Some("llm_plan_forbidden"))
          assertEquals(r.results, Nil)
        }
      }
    }

  test("C34 扩展（P1-3）：llm ingress 提交含桥接件名（dev:tool:*）的 Pipe/Sequence 计划 ⇒ 同样拒绝——llmPlanCheck 面覆盖桥接件"):
    IrTestKit.vfs().flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        // 命令名取自桥接表（真实 dev:tool:* 面）；llmPlanCheck 先于节点收集 ⇒ 无需注册即拒
        val pipe = Ir.Pipe(List(IrTestKit.call("dev:tool:read"), IrTestKit.call("dev:tool:grep")))
        val seq = Ir.Sequence(List(IrTestKit.call("dev:tool:write"), IrTestKit.call("dev:tool:bash")))
        for
          r1 <- run(h, pipe, tenant = Tenant.Llm("sess", "Nebula"), ingress = Ingress.Llm)
          r2 <- run(h, seq, tenant = Tenant.Llm("sess", "Nebula"), ingress = Ingress.Llm)
        yield
          assertEquals(r1.status, Status.Invalid)
          assertEquals(r1.error.flatMap(_.details("reason").flatMap(_.asString)), Some("llm_plan_forbidden"))
          assertEquals(r1.results, Nil)
          assertEquals(r2.status, Status.Invalid)
          assertEquals(r2.error.flatMap(_.details("reason").flatMap(_.asString)), Some("llm_plan_forbidden"))
          assertEquals(r2.results, Nil)
      }
    }

  test("§4.6 软链接/重解析点：根内别名指向根内 ⇒ 放行（按真实落点判定）；指向根外 ⇒ 计划非法"):
    IrTestKit.vfs("real.txt" -> "A").flatMap { root =>
      val outside = os.temp.dir(prefix = "nebflow-ir-outside-")
      os.write(outside / "secret.txt", "TOP SECRET")
      val aliasCreated =
        try
          os.symlink(root / "alias.txt", root / "real.txt")
          true
        catch
          case _: Exception => false
      // 根外逃逸向量：先试文件符号链接；Windows 无特权时退回**目录 junction**
      // （`mklink /J` 不需要管理员/开发者模式，且同样被 realpath 解析 —— 逃逸面等价）
      val escapePath = root / "escape"
      val escapeCreated =
        try
          os.symlink(escapePath, outside)
          true
        catch
          case _: Exception =>
            try
              os.proc("cmd", "/c", "mklink", "/J", escapePath.toString, outside.toString).call(check = false)
              os.exists(escapePath)
            catch case _: Exception => false
      assume(aliasCreated || escapeCreated, "no symlink/junction privilege on this host")
      IrTestKit.harness(root).flatMap { h =>
        val inRoot = Ir.Redirect(RedirectOp.Out, "copy.txt", IrTestKit.call("dev:fs:cat", Some("alias.txt")))
        for
          ok <- if aliasCreated then run(h, inRoot) else IO.pure(PlanResult("skip", Status.Done, Some(0)))
          escaped <- run(h, IrTestKit.call("dev:fs:cat", Some("escape/secret.txt")))
        yield
          if aliasCreated then assertEquals(file(root, "copy.txt"), "A") // realpath 先解析，别名读到真身
          assertEquals(ok.status, Status.Done)
          assertEquals(escaped.status, Status.Invalid) // 真实落点在根外 ⇒ 计划非法
          assertEquals(escaped.error.map(_.code), Some(Codes.SchemaBadValue))
          assertEquals(escaped.results, Nil) // 零执行：宿主文件没有被读
      }
    }

  test("dev:fs:cat 的 path 语义：VFS 相对、`/` 前缀同为 VFS 根、缺省读 stdin"):
    IrTestKit.vfs("sub/a.txt" -> "A").flatMap { root =>
      IrTestKit.harness(root).flatMap { h =>
        val byRelative = Ir.Redirect(RedirectOp.Out, "r1.txt", IrTestKit.call("dev:fs:cat", Some("sub/a.txt")))
        val byRootAbs = Ir.Redirect(RedirectOp.Out, "r2.txt", IrTestKit.call("dev:fs:cat", Some("/sub/a.txt")))
        val fromStdin = Ir.Pipe(
          List(IrTestKit.call("dev:fs:ls"), IrTestKit.call("dev:fs:cat"))
        )
        for
          _ <- run(h, byRelative)
          _ <- run(h, byRootAbs)
          r3 <- run(h, fromStdin)
        yield
          assertEquals(file(root, "r1.txt"), "A")
          assertEquals(file(root, "r2.txt"), "A")
          assert(r3.results(1).stdout.get.asInstanceOf[StreamValue.Text].value.nonEmpty)
      }
    }

end IrExecSpec
