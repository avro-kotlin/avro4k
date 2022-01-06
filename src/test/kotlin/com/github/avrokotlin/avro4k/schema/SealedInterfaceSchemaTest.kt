package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.polymorphicTreeForSealed
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

sealed interface Calculable
sealed interface UnaryCalculable : Calculable {
   val value : Int
}
sealed interface BinaryCalculable : Calculable {
   val left: Int
   val right: Int
}
@Serializable
object NullaryCalculable : Calculable
@Serializable
data class NegateCalculable(override val value : Int) : UnaryCalculable
@Serializable
data class AddCalculable(override val left : Int, override val right : Int) : BinaryCalculable
@Serializable
data class SubstractCalculable(override val left : Int, override val right : Int) : BinaryCalculable


@Serializable
data class ReferencingSealedInterface(
   val notNullable: Calculable
)
@Serializable
data class ReferencingNullableSealedInterface(
   @AvroDefault(Avro.NULL)
   val nullable : Calculable?
)

@OptIn(InternalSerializationApi::class)
class SealedInterfaceSchemaTest : StringSpec({

   "referencing a sealed hierarchy"{
      val schema = Avro(serializersModule = SerializersModule {
         polymorphicTreeForSealed(Calculable::class)
      }).schema(ReferencingSealedInterface.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/sealed_interface_hierarchy_referenced.json"))
      schema shouldBe expected
   }
   "referencing a nullable sealed hierarchy"{
      val schema = Avro(serializersModule = SerializersModule {
         polymorphicTreeForSealed(Calculable::class)
      }).schema(ReferencingNullableSealedInterface.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/sealed_interface_nullable_referenced.json"))
      schema shouldBe expected
   }
   "referencing a sealed interface"{
      val schema = Avro.default.schema(ReferencingSealedInterface.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/sealed_interface_referenced.json"))
      schema shouldBe expected
   }
})

