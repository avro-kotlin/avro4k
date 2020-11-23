package com.github.avrokotlin.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroName
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class AvroNameSchemaTest : FunSpec({

  test("generate field names using @AvroName") {

    @Serializable
    data class Foo(@AvroName("foo") val wibble: String, val wobble: String)

    val schema = Avro.default.schema(Foo.serializer())
    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_name_field.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate class names using @AvroName") {

    @AvroName("wibble")
    @Serializable
    data class Foo(val a: String, val b: String)

    val schema = Avro.default.schema(Foo.serializer())
    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_name_class.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

})