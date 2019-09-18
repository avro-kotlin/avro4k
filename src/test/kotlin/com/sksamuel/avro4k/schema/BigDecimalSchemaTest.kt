package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.ContextualSerialization
import kotlinx.serialization.Serializable
import java.math.BigDecimal


//class BigDecimalSchemaTest : FunSpec({
//
//  test("accept big decimal as logical type on bytes") {
//
//    @Serializable
//    data class Test(@ContextualSerialization val decimal: BigDecimal)
//
//    val schema = Avro.default.schema(Test.serializer())
//    val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/bigdecimal.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }
//})

