package nebflow.ir

import io.circe.syntax.*
import io.circe.{Json, JsonObject}

/**
 * `argsSchema` 子集校验（标准 §7.6）。
 *
 * v1 支持的关键字白名单：`type`、`properties`、`required`、`enum`、`items`、`description`、
 * `default`、`minimum`/`maximum`、`pattern`、`additionalProperties:false`。**未知关键字
 * 应当忽略**（schema 是外部提供物，不比 IR 线格式；与 [I8] 的严格范围不同）。
 *
 * 校验失败**必须**在执行**前**发生，details **必须**逐条列出违规路径（便于模型自纠）。
 * **默认值不注入**：`default` 只作文档/糖面提示 —— `dev:fs:cat` 的「`path` 省略 ⇒ 读
 * stdin」（§7.7）依赖「缺省」与「显式 `.`」可区分，注入默认值会把两者混为一谈。
 */
object ArgsSchema:

  def validate(schema: JsonObject, args: JsonObject): Either[IrError, Unit] =
    val violations = check(schema, args, "")
    if violations.isEmpty then Right(())
    else
      Left(
        IrError
          .invalidArgs(s"args do not satisfy the command schema (${violations.length} violation(s))", violations)
          .withDetail("reason", "schema_violation".asJson)
      )

  private def check(schema: JsonObject, args: JsonObject, prefix: String): List[String] =
    val out = List.newBuilder[String]

    // additionalProperties:false ⇒ 多余键（§7.6）
    val declared = schema("properties").flatMap(_.asObject).map(_.keys.toSet).getOrElse(Set.empty)
    val extraClosed = schema("additionalProperties").flatMap(_.asBoolean).contains(false)
    if extraClosed then
      args.keys.filterNot(declared.contains).toList.sorted.foreach { k =>
        out += s"$prefix/$k: not allowed (additionalProperties=false)"
      }

    // required 缺键
    val required = schema("required").flatMap(_.asArray).map(_.flatMap(_.asString).toList).getOrElse(Nil)
    required.filterNot(args.contains).foreach { k =>
      out += s"$prefix/$k: required"
    }

    val props = schema("properties").flatMap(_.asObject)
    props.foreach { ps =>
      ps.toIterable.foreach { case (key, propSchema) =>
        args(key).foreach { value =>
          val propObj = propSchema.asObject.getOrElse(JsonObject.empty)
          val path = s"$prefix/$key"
          propObj("type").flatMap(_.asString).foreach { t =>
            if !typeMatches(t, value) then out += s"$path: expected $t"
          }
          propObj("enum").flatMap(_.asArray).foreach { allowed =>
            if !allowed.contains(value) then out += s"$path: not one of the allowed values"
          }
          propObj("pattern").flatMap(_.asString).foreach { re =>
            value.asString.foreach { s =>
              if re.r.findFirstIn(s).isEmpty then out += s"$path: does not match $re"
            }
          }
          pair(propObj, "minimum").foreach { case (name, bound) =>
            (value.asNumber.flatMap(_.toBigDecimal), bound.asNumber.flatMap(_.toBigDecimal)) match
              case (Some(n), Some(b)) if n < b => out += s"$path: below $name $b"
              case _ => ()
          }
          pair(propObj, "maximum").foreach { case (name, bound) =>
            (value.asNumber.flatMap(_.toBigDecimal), bound.asNumber.flatMap(_.toBigDecimal)) match
              case (Some(n), Some(b)) if n > b => out += s"$path: above $name $b"
              case _ => ()
          }
          propObj("items").flatMap(_.asObject).foreach { itemSchema =>
            value.asArray.foreach { arr =>
              arr.zipWithIndex.foreach { case (item, i) =>
                item.asObject.foreach { io =>
                  out ++= check(itemSchema, io, s"$path/$i")
                }
              }
            }
          }
        }
      }
    }
    out.result()

  end check

  private def pair(schema: JsonObject, key: String): Option[(String, Json)] =
    schema(key).map(v => (key, v))

  private def typeMatches(t: String, value: Json): Boolean = t match
    case "string" => value.isString
    case "number" => value.isNumber
    case "integer" => value.asNumber.exists(_.toBigDecimal.exists(_.isWhole))
    case "boolean" => value.isBoolean
    case "object" => value.isObject
    case "array" => value.isArray
    case "null" => value.isNull
    case _ => true // 未知 type 关键字：忽略（§7.6）

end ArgsSchema
