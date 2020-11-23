package com.github.avrokotlin.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroDoc
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable

class AvroDocSchemaTest : WordSpec({

  "@AvroDoc" should {
    "support doc annotation on class"  {
      @AvroDoc("hello; is it me youre looking for")
      @Serializable
      data class Annotated(val str: String)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/doc_annotation_class.json"))
      val schema = Avro.default.schema(Annotated.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support doc annotation on field"  {
      @Serializable
      data class Annotated(@AvroDoc("hello its me") val str: String, @AvroDoc("I am a long") val long: Long, val int: Int)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/doc_annotation_field.json"))
      val schema = Avro.default.schema(Annotated.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support doc annotation on nested class"  {
      @Serializable
      data class Nested(@AvroDoc("b") val foo: String)

      @Serializable
      data class Annotated(@AvroDoc("c") val nested: Nested)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/doc_annotation_field_struct.json"))
      val schema = Avro.default.schema(Annotated.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
  }

})