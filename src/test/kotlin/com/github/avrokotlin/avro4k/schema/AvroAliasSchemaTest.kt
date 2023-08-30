package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class AvroAliasSchemaTest : WordSpec({

  "SchemaEncoder" should {
    "support alias annotations on types"  {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/aliases_on_types.json"))
      val schema = Avro.default.schema(TypeAnnotated.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support multiple alias annotations on types"  {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/aliases_on_types_multiple.json"))
      val schema = Avro.default.schema(TypeAliasAnnotated.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support alias annotations on field"  {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/aliases_on_fields.json"))
      val schema = Avro.default.schema(FieldAnnotated.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
     "support multiple alias annotations on fields"  {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/aliases_on_fields_multiple.json"))
        val schema = Avro.default.schema(FieldAliasAnnotated.serializer())
        schema.toString(true) shouldBe expected.toString(true)
     }
  }

}) {
    @Serializable
    @AvroAlias("queen")
    data class TypeAnnotated(val str: String)

    @AvroAlias("queen", "ledzep")
    @Serializable
    data class TypeAliasAnnotated(val str: String)

    @Serializable
    data class FieldAnnotated(@AvroAlias("cold") val str: String, @AvroAlias("kate") val long: Long, val int: Int)

    @Serializable
    data class FieldAliasAnnotated(@AvroAlias("queen", "ledzep") val str: String)
}
