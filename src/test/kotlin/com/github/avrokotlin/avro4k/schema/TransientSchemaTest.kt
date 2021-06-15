package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class TransientSchemaTest : FunSpec({

  test("@AvroTransient fields should be ignored") {

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/transient.json"))
    val schema = Avro.default.schema(TransientFoo.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

}) {
  @Serializable
  data class TransientFoo(val a: String, @kotlinx.serialization.Transient val b: String = "foo", val c: String)
}
