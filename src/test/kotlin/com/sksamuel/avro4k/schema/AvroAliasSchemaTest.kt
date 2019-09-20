package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroAlias
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import kotlinx.serialization.Serializable

class AvroAliasSchemaTest : WordSpec({

  "SchemaEncoder" should {
    "support alias annotations on types"  {

      @Serializable
      @AvroAlias("queen")
      data class Annotated(val str: String)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/aliases_on_types.json"))
      val schema = Avro.default.schema(Annotated.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
//    "support multiple alias annotations on types"  {
//
//      @AvroAlias("queen")
//      @AvroAlias("ledzep")
//      @Serializable
//      data class Annotated(val str: String)
//
//      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/aliases_on_types_multiple.json"))
//      val schema = Avro.default.schema(Annotated.serializer())
//      schema.toString(true) shouldBe expected.toString(true)
//    }
    "support alias annotations on field"  {

      @Serializable
      data class Annotated(@AvroAlias("cold") val str: String, @AvroAlias("kate") val long: Long, val int: Int)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/aliases_on_fields.json"))
      val schema = Avro.default.schema(Annotated.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
  }

})