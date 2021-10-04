package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.*
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass

@Serializable
data class MixedPolymoprhicIoReference (
   val expr : Expr,
   val nullableExpr : Expr?,
   val listExpr : List<Expr>,
   val mapExpr : Map<String, Expr>
)
class MixedPolymorphicIoTest : StringSpec({
   "read / write referencing a mixed sealed hierarchy" {
      val module = SerializersModule {
         polymorphic(OtherUnaryExpr::class) {
            subclass(ConstExpr::class)
         }
         polymorphic(OtherBinaryExpr::class) {
            subclass(MultiplicationExpr::class)
         }
      }
      val avro = Avro(serializersModule = module)
      val toWrite = MixedPolymoprhicIoReference(NegateExpr(3), null, listOf(NegateExpr(1)), mapOf("blub" to NullaryExpr))
      writeRead(toWrite, MixedPolymoprhicIoReference.serializer(), avro)
   }
})
