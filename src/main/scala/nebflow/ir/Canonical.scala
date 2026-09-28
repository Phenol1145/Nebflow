/* 命令 IR 路由层（Command IR Router，P0）——规范面 `.zcode/plans/command-ir-standard.md` v0.1。
 *
 * 本包层位：`shared < actor < ir < core < …`（scripts/check-scala-layers.mjs LAYER_ORDER）。
 * **只准**依赖 shared/actor；禁止 import core/agent/gateway 等上层——跨层数据在
 * 适配点（gateway/Core）单向注入（VFS 根解析、安全档、审计落点）。
 */
package nebflow.ir

import io.circe.{Json, JsonNumber}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * 单点 JSON 打印器 + 摘要（标准 §4.1 / §4.5 / [D27]）。
 *
 * **禁止**用 circe 默认打印器做摘要输入：[D27] 实测两条违 JCS —— `JsonObject` 保留
 * 插入序（键序影响输出）且 `JsonNumber` 保留原始字面量（`1.0` / `1e2` 原样输出）。
 * 本对象是「键排序 + 紧凑 + 最短往返」的**唯一**实现，摘要（§4.5）与 `jsonl→text`
 * 序列化（§5.3）共用它——否则同一个值会出现两种文本形态，摘要与展示对不上。
 *
 * 正解先例：`core/processor/LoopGuard.scala:140 canonicalize`（手写 sortBy + noSpaces）。
 * 与 JCS（RFC 8785）的**已知偏离**（v1 记录在案）：极值数字（|指数| 很大时 JCS 用
 * `1e+30` 形态，本实现给全展开的十进制）。摘要稳定性（同值同摘要）不受影响。
 */
object Canonical:

  /** 键排序、紧凑、数值最短往返的 JSON 文本。 */
  def print(json: Json): String =
    val sb = new StringBuilder
    write(json, sb)
    sb.toString

  private def write(json: Json, sb: StringBuilder): Unit =
    json.fold(
      jsonNull = sb.append("null"),
      jsonBoolean = b => sb.append(if b then "true" else "false"),
      jsonNumber = n => sb.append(numberText(n)),
      // 字符串转义复用 circe（noSpaces 对单个字符串 = 带引号的转义形态）
      jsonString = s => sb.append(Json.fromString(s).noSpaces),
      jsonArray = arr =>
        sb.append('[')
        var first = true
        arr.foreach { v =>
          if !first then sb.append(',')
          first = false
          write(v, sb)
        }
        sb.append(']')
      ,
      jsonObject = obj =>
        sb.append('{')
        var first = true
        obj.toIterable.toList.sortBy(_._1).foreach { case (k, v) =>
          if !first then sb.append(',')
          first = false
          sb.append(Json.fromString(k).noSpaces).append(':')
          write(v, sb)
        }
        sb.append('}')
    )

  /**
   * 最短往返数值文本：整数（含 `1e2` → `100`、`0.0` → `0`）走 BigInteger，
   * 其余 stripTrailingZeros 后 toPlainString。非十进制可表示的字面量原样输出。
   */
  private def numberText(n: JsonNumber): String =
    n.toBigDecimal match
      case Some(bd) =>
        val stripped = bd.bigDecimal.stripTrailingZeros
        if stripped.scale <= 0 then stripped.toBigInteger.toString else stripped.toPlainString
      case None => n.toString

  /** `sha256(UTF8(JCS(json)))`，十六进制小写全串（§4.5）。 */
  def digest(json: Json): String = sha256Hex(print(json))

  /** 含 `Secret` cap 的命令用 HMAC 而非明文 sha256（§8.2：低熵凭证可离线字典破解）。 */
  def hmacDigest(json: Json, key: Array[Byte]): String = hmacSha256Hex(print(json), key)

  def sha256Hex(text: String): String =
    hex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)))

  def hmacSha256Hex(text: String, key: Array[Byte]): String =
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key, "HmacSHA256"))
    hex(mac.doFinal(text.getBytes(StandardCharsets.UTF_8)))

  private def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & 0xff}%02x").mkString

end Canonical
