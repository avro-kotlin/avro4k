package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.apache.avro.Schema

@Serializable
abstract class UnsealedPolymorphicRoot

@Serializable
data class UnsealedChildOne(val one: String) : UnsealedPolymorphicRoot()

@Serializable
data class UnsealedChildTwo(val two: String) : UnsealedPolymorphicRoot()

class PolymorphicClassSchemaTest : StringSpec({
   "schema for polymorphic hierarchy" {
      val module = SerializersModule {
         polymorphic(UnsealedPolymorphicRoot::class) {
            subclass(UnsealedChildOne::class)
            subclass(UnsealedChildTwo::class)
         }
      }
      val schema = Avro(serializersModule = module).schema(UnsealedPolymorphicRoot.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/polymorphic.json"))
      schema shouldBe expected
   }
})