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
sealed class SealedChildTwo : UnsealedPolymorphicRoot()
@Serializable
data class UnsealedChildTwo(val two: String) : SealedChildTwo()

@Serializable
data class ReferencingPolymorphicRoot(
   val root : UnsealedPolymorphicRoot,
   val nullableRoot : UnsealedPolymorphicRoot? = null
)

@Serializable
data class PolymorphicRootInList(
   val listOfRoot : List<UnsealedPolymorphicRoot>
)

@Serializable
data class PolymorphicRootInMap(
   val mapOfRoot : Map<String, UnsealedPolymorphicRoot>
)
val polymorphicModule = SerializersModule {
   polymorphic(UnsealedPolymorphicRoot::class) {
      subclass(UnsealedChildOne::class)
      subclass(UnsealedChildTwo::class)
   }
}

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
      val schema = Avro(serializersModule = polymorphicModule).schema(ReferencingPolymorphicRoot.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/polymorphic_reference.json"))
      schema shouldBe expected
   }
   
   "Supports polymorphic references in lists" {      
      val schema = Avro(serializersModule = polymorphicModule).schema(PolymorphicRootInList.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/polymorphic_reference_list.json"))
      schema shouldBe expected
   }
   
   "Supports polymorphic references in maps" {      
      val schema = Avro(serializersModule = polymorphicModule).schema(PolymorphicRootInMap.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/polymorphic_reference_map.json"))
      schema shouldBe expected
   }
})