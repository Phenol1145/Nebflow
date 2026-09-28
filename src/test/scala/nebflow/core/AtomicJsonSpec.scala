package nebflow.core

import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite

import scala.concurrent.duration.*

/**
 * AtomicJson 契约：tmp + rename(2)——写后内容完整、旧内容不被破坏、目录无
 * *.tmp.* 残留（成功路径）、IO 形态与 Sync 形态行为一致。
 */
class AtomicJsonSpec extends CatsEffectSuite:

  override def munitIOTimeout: FiniteDuration = 30.seconds

  test("write creates the file with exact content (nested dirs auto-created)") {
    for
      dir <- IO(os.temp.dir())
      f = dir / "deep" / "nested" / "state.json"
      _ <- AtomicJson.write(f, """{"a":1}""")
      content <- IO(os.read(f))
    yield assertEquals(content, """{"a":1}""")
  }

  test("write replaces existing content atomically — old or new, never partial") {
    for
      dir <- IO(os.temp.dir())
      f = dir / "state.json"
      _ <- AtomicJson.write(f, """{"version":1}""")
      _ <- AtomicJson.write(f, """{"version":2,"x":"yyyy"}""")
      content <- IO(os.read(f))
      residue <- IO(os.list(dir).filter(_.last.contains(".tmp.")))
    yield
      assertEquals(content, """{"version":2,"x":"yyyy"}""")
      assertEquals(residue.toList, List.empty[os.Path], s"no tmp residue expected: $residue")
  }

  test("writeSync behaves identically to write") {
    for
      dir <- IO(os.temp.dir())
      f = dir / "sync.json"
      _ <- IO(AtomicJson.writeSync(f, "[1,2,3]"))
      c1 <- IO(os.read(f))
      _ <- IO(AtomicJson.writeSync(f, "[4]"))
      c2 <- IO(os.read(f))
      residue <- IO(os.list(dir).filter(_.last.contains(".tmp.")))
    yield
      assertEquals(c1, "[1,2,3]")
      assertEquals(c2, "[4]")
      assertEquals(residue.toList, List.empty[os.Path])
  }

  test("failed move leaves the old file intact and cleans up the tmp file") {
    // 构造必失败场景：把目标路径的父目录换成文件 → tmp 写入必败。
    // 断言：异常抛出（不吞）、旧文件内容原样、目录无 tmp 残留。
    IO(os.temp.dir()).flatMap { dir =>
      val blocker = dir / "blocker"
      os.write.over(blocker, "i am a file")
      val f = blocker / "sub" / "state.json" // 父目录是文件 → tmp 写不进去 → 必败
      IO(AtomicJson.writeSync(f, "{}")).attempt.flatMap {
        case Left(_) => IO(assert(true, "failure is raised, not swallowed"))
        case Right(_) => IO(fail("expected the write to fail"))
      } *> IO {
        assertEquals(os.read(blocker), "i am a file", "blocker file untouched")
      }
    }
  }

  test("transient AccessDenied on the rename target is retried, not surfaced (Windows handle race)") {
    // 2026-09-29 全量回归修复的回归钉：Windows 的 rename-replace 需要对目标的
    // delete 访问，而 Java NIO 读句柄默认不带 FILE_SHARE_DELETE ⇒ 目标正被读取
    // 时一次性 move 回 AccessDeniedException（POSIX rename 则允许）。实测形态 =
    // TriggerChainSpec T-C：detached trigger 写 flow-map.json 被并发读方拒绝。
    // 断言：读方在重试窗口内（这里 ~50ms < 总退避 ~200ms）放手 ⇒ 写成功且内容
    // 完整、无 tmp 残留。POSIX 上本场景一次 move 即成功，用例平凡通过（不证
    // 伪，只保 Windows 侧不回归）。
    for
      dir <- IO(os.temp.dir())
      f = dir / "raced.json"
      _ <- IO(AtomicJson.writeSync(f, "{\"v\":1}"))
      // 读句柄在另一线程持有 50ms 后关闭；写方与之并发
      readerFiber <- IO {
        val in = java.nio.file.Files.newInputStream(f.toNIO)
        in.read() // 真正打开句柄
        in
      }.flatMap { in =>
        IO.sleep(50.millis) *> IO(in.close())
      }.start
      _ <- IO(AtomicJson.writeSync(f, "{\"v\":2}")).guarantee(readerFiber.join.void)
      content <- IO(os.read(f))
      residue <- IO(os.list(dir).filter(_.last.contains(".tmp.")))
    yield
      assertEquals(content, "{\"v\":2}")
      assertEquals(residue.toList, List.empty[os.Path])
  }

end AtomicJsonSpec
