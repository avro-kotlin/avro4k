package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroFixed
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable

class AvroFixedSchemaTest : WordSpec({

   "@AvroFixed" should {

      "generated fixed field schema when used on a field"  {

         val schema = Avro.default.schema(FixedStringField.serializer())
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/fixed_string.json"))
         schema.toString(true) shouldBe expected.toString(true)
      }

      "generated fixed schema when an annotated type is used as the type in a field"  {

         val schema = Avro.default.schema(Foo.serializer())
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/fixed_string_value_type_as_field.json"))
         schema.toString(true) shouldBe expected.toString(true)
      }
   }
}) {
   @Serializable
   data class FixedStringField(@AvroFixed(7) val mystring: String)

   @AvroFixed(8)
   @Serializable
   data class FixedClass(val bytes: ByteArray)

   @Serializable
   data class Foo(val z: FixedClass)
}
