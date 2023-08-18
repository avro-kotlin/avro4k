package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroName
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class MultipleGenericsSchemaTest : FunSpec({

  test("encode different generics") {

    val schema = Avro.default.schema(WrapperTest.serializer())
    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/multiple_generics.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }


}) {
  @Serializable
  data class GenericValue<T>(val z: T)

  @Serializable
  @AvroName("WrapperTest")
  data class WrapperTest(val a: GenericValue<String>, val b: GenericValue<Double>)
}
