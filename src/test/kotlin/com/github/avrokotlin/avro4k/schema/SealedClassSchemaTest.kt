package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDefault
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema

@Serializable
sealed class Operation {
   @Serializable
   object Nullary : Operation()

   @Serializable
   sealed class Unary() : Operation(){
      abstract val value : Int
      @Serializable
      data class Negate(override val value:Int) : Unary()
   }

   @Serializable
   sealed class Binary() : Operation(){
      abstract val left : Int
      abstract val right : Int
      @Serializable
      data class Add(override val left : Int, override val right : Int) : Binary()
      @Serializable
      data class Substract(override val left : Int, override val right : Int) : Binary()
   }
}
@Serializable
data class ReferencingSealedClass(
   val notNullable: Operation
)
@Serializable
data class ReferencingNullableSealedClass(
   @AvroDefault(Avro.NULL)
   val nullable : Operation?
)

class SealedClassSchemaTest : StringSpec({

   "schema for sealed hierarchy" {
      val schema = Avro.default.schema(Operation.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/sealed.json"))
      schema shouldBe expected
   }
   "referencing a sealed hierarchy"{
      val schema = Avro.default.schema(ReferencingSealedClass.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/sealed_referenced.json"))
      schema shouldBe expected
   }
   "referencing a nullable sealed hierarchy"{
      val schema = Avro.default.schema(ReferencingNullableSealedClass.serializer())
      val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/sealed_nullable_referenced.json"))
      schema shouldBe expected
   }
})