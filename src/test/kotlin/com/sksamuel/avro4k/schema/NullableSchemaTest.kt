package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class NullableSchemaTest : FunSpec({

  test("generate null as Union[T, Null]") {

    @Serializable
    data class Test(val nullableString: String?, val nullableBoolean: Boolean?)

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/nullables.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

//  test("move default option values to first schema as per avro spec") {
//    val schema = AvroSchema[OptionWithDefault]
//    val expected = new org . apache . avro . Schema . Parser ().parse(getClass.getResourceAsStream("/option_default_value.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }
//
//  test("if a field has a default value of null then define the field to be nullable") {
//    val schema = AvroSchema[FieldWithNull]
//    val expected = new org . apache . avro . Schema . Parser ().parse(getClass.getResourceAsStream("/option_from_null_default.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }

})