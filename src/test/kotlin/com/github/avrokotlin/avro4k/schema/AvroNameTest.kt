package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroName
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class AvroNameSchemaTest : FunSpec({

  test("generate field names using @AvroName") {

    val schema = Avro.default.schema(FieldNamesFoo.serializer())
    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_name_field.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate class names using @AvroName") {

    val schema = Avro.default.schema(ClassNameFoo.serializer())
    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_name_class.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

}) {

  @Serializable
  data class FieldNamesFoo(@AvroName("foo") val wibble: String, val wobble: String)

  @AvroName("wibble")
  @Serializable
  data class ClassNameFoo(val a: String, val b: String)
}
