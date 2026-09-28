package nebflow.core

import cats.effect.IO

/**
 * Crash-safe file writes for JSON (or any small text) state files.
 *
 * Writes go to a unique temp file in the target's directory, then
 * `java.nio.file.Files.move` with `ATOMIC_MOVE` — which maps to rename(2),
 * a true single-step atomic replace on POSIX. Readers therefore see either
 * the complete old file or the complete new file, never a truncated/partial
 * write left behind by a mid-write crash (issue #23).
 *
 * Why not `os.move.over(replaceExisting = true)`: os-lib implements that as
 * delete-then-rename, which is NOT atomic (and throws NoSuchFileException if
 * the target disappears between the two steps). The old tmp+move.over
 * precedents only narrowed the corruption window — this closes it.
 *
 * Concurrency: the temp name carries a random UUID, so concurrent writers
 * never share a temp file (the shared `_index.json.tmp` race — see
 * SessionStore.saveIndex history).
 */
object AtomicJson:

  /** Windows 瞬态句柄竞态的有界重试参数（见 [[moveAtomically]]）。 */
  private val MoveRetryAttempts = 5
  private val MoveRetryBackoffMs = 20L

  /**
   * Windows 瞬态句柄竞态的宽容重试（2026-09-29，全量回归 TriggerChainSpec T-C 修复）：
   * POSIX rename 允许替换「正被打开读取」的目标；Windows 对同一操作回
   * `AccessDeniedException`（另一个读方/杀软扫描短暂持有目标句柄——全量回归负载下
   * 的实测形态：`flow-map.json.tmp.<uuid> -> flow-map.json` 被拒，detached trigger
   * 失败、waitUntil 超时）。有界重试（短线性退避，总上限 ~200ms）只**重放同一个
   * 原子 move**，不改原子-or-fail 语义；超限仍抛出原异常（fail-loud 不变）。
   */
  private def moveAtomically(
      tmp: java.nio.file.Path,
      target: java.nio.file.Path,
      options: Seq[java.nio.file.StandardCopyOption],
      attempt: Int = 1
  ): Unit =
    try java.nio.file.Files.move(tmp, target, options*)
    catch
      case e: java.nio.file.AccessDeniedException if attempt < MoveRetryAttempts =>
        Thread.sleep(MoveRetryBackoffMs * attempt)
        moveAtomically(tmp, target, options, attempt + 1)

  /** Effectful write — runs on the blocking thread pool. */
  def write(path: os.Path, content: String): IO[Unit] =
    IO.blocking(writeSync(path, content))

  /**
   * Direct blocking write for call sites already inside `IO.blocking`, or
   * synchronous code (ModelRegistry-style). Same semantics as [[write]].
   */
  def writeSync(path: os.Path, content: String): Unit =
    val tmp = path / os.up / s"${path.last}.tmp.${java.util.UUID.randomUUID()}"
    try
      os.write.over(tmp, content, createFolders = true)
      moveAtomically(
        tmp.toNIO,
        path.toNIO,
        Seq(
          java.nio.file.StandardCopyOption.REPLACE_EXISTING,
          java.nio.file.StandardCopyOption.ATOMIC_MOVE
        )
      )
    catch
      case e: Throwable =>
        // On failure the target file is untouched — that is the point of the
        // tmp+rename dance. Best-effort cleanup so failed writes don't litter
        // the directory with *.tmp.* residue.
        try if os.exists(tmp) then os.remove(tmp)
        catch case _: Exception => ()
        throw e
    end try
  end writeSync

  /**
   * Durability-first variant of [[writeSync]] for caches and state files whose
   * loss on a crash-window matters more than fail-loud atomicity: the temp
   * file is fsync'd (`FileChannel.force(true)`) before the rename, and a
   * filesystem without atomic-move support degrades to a plain replacing
   * move instead of failing the write (usage-agg / device-profiles precedent).
   * Callers wanting strict atomic-or-fail semantics should use [[writeSync]].
   */
  def writeSyncDurable(path: os.Path, content: String): Unit =
    val tmp = path / os.up / s"${path.last}.tmp.${java.util.UUID.randomUUID()}"
    try
      os.write.over(tmp, content, createFolders = true)
      val ch = java.nio.channels.FileChannel.open(tmp.toNIO, java.nio.file.StandardOpenOption.WRITE)
      try ch.force(true)
      finally ch.close()
      try
        moveAtomically(tmp.toNIO, path.toNIO, Seq(java.nio.file.StandardCopyOption.ATOMIC_MOVE))
      catch
        case _: java.nio.file.AtomicMoveNotSupportedException =>
          // Providers without atomic move support: degrade to a replacing
          // move rather than losing the write.
          java.nio.file.Files.move(
            tmp.toNIO,
            path.toNIO,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING
          )
      end try
    catch
      case e: Throwable =>
        try if os.exists(tmp) then os.remove(tmp)
        catch case _: Exception => ()
        throw e
    end try
  end writeSyncDurable

end AtomicJson
