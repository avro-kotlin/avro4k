package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.apache.avro.Schema
@Serializable
abstract class UnsealedPolymorphicRoot

@Serializable
data class UnsealedChildOne(val one: String) : UnsealedPolymorphicRoot()

@Serializable
sealed class SealedChildTwo : UnsealedPolymorphicRoot()
@Serializable
data class UnsealedChildTwo(val two: String) : SealedChildTwo()

@Serializable
data class ReferencingPolymorphicRoot(
   val root : UnsealedPolymorphicRoot,
   val nullableRoot : UnsealedPolymorphicRoot? = null
)

class PolymorphicClassSchemaTest : StringSpec({
   "schema for polymorphic hierarchy" {
      val module = SerializersModule {
         polymorphic(UnsealedPolymorphicRoot::class) {
            subclass(UnsealedChildOne::class)
            subclass(SealedChildTwo::class)
         }
      }
      val schema = Avro(serializersModule = module).schema(UnsealedPolymorphicRoot.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/polymorphic.json"))
      schema shouldBe expected
   }

   "supports polymorphic references / nested fields" {
      val module = SerializersModule {
         polymorphic(UnsealedPolymorphicRoot::class) {
            subclass(UnsealedChildOne::class)
            subclass(UnsealedChildTwo::class)
         }
      }
      val schema = Avro(serializersModule = module).schema(ReferencingPolymorphicRoot.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/polymorphic_reference.json"))
      schema shouldBe expected
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