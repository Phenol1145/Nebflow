package nebflow.ir

/**
 * 线格式常量（标准 §10.1）：`ir` 版本是**信封属性**，节点内不带版本。
 *
 * 单独成文件是**刻意的**：`Errors.scala`（响应信封）也要写版本，若把常量留在
 * `Ir.scala`，就会形成 `Errors ⇄ Ir` 的**文件级**环（`Ir` 反过来要用 `Errors` 的错误码
 * 与 `Names` 的语法检查）——常量下沉到这个零依赖叶子文件，包内文件的依赖图保持无环。
 */
object IrWire:

  /** 实现支持的版本；消费者**必须**拒绝不支持的版本（`router.schema.bad_version`，[I8]）。 */
  val Version = 1

end IrWire
