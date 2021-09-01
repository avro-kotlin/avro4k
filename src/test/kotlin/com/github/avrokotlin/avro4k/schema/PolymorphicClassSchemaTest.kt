package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonTransformingSerializer
import kotlinx.serialization.json.buildJsonObject
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

   "polymorphic defaults are not supported" {
      val module = SerializersModule {
         polymorphic(UnsealedPolymorphicRoot::class) {
            subclass(UnsealedChildOne::class)
            subclass(UnsealedChildTwo::class)
            default { TypeSerializer }
         }
      }
      val exception = shouldThrow<SerializationException> {
         Avro(serializersModule = module).schema(UnsealedPolymorphicRoot.serializer())
      }
      exception.message shouldBe "Polymorphic defaults are not supported in avro4k. Error whilst describing: UnsealedPolymorphicRoot"
   }
})

object TypeSerializer : JsonTransformingSerializer<UnsealedPolymorphicRoot>(UnsealedPolymorphicRoot.serializer()) {
   override fun transformDeserialize(element: JsonElement): JsonElement {
      return when (element) {
         is JsonPrimitive -> {
            buildJsonObject {
               put("type", element)
            }
         }
         is JsonObject -> element
         else -> throw IllegalAccessException("Unsupported json element type")
      }
   }
}