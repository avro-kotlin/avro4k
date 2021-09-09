package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.ConstExpr
import com.github.avrokotlin.avro4k.schema.MultiplicationExpr
import com.github.avrokotlin.avro4k.schema.NegateExpr
import com.github.avrokotlin.avro4k.schema.OtherBinaryExpr
import com.github.avrokotlin.avro4k.schema.OtherUnaryExpr
import com.github.avrokotlin.avro4k.schema.ReferencingMixedPolymorphic
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.apache.avro.generic.GenericRecord

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
      writeRead(ReferencingMixedPolymorphic(NegateExpr(3)), ReferencingMixedPolymorphic.serializer(), avro)
      writeRead(ReferencingMixedPolymorphic(NegateExpr(3)), ReferencingMixedPolymorphic.serializer(), avro) {
         val reference = it["notNullable"] as GenericRecord
         reference.schema shouldBe avro.schema(NegateExpr.serializer())
         reference["value"] shouldBe 3
      }
   }
})
