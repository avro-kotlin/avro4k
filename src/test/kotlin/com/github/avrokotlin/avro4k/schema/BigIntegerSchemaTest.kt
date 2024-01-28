@file:UseSerializers(BigIntegerSerializer::class)

package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.serializer.BigIntegerSerializer
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.math.BigInteger

class BigIntegerSchemaTest : FunSpec({

    test("accept big integer as String") {

        val schema = Avro.default.schema(Test.serializer())
        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigint.json"))
        schema shouldBe expected
    }

    test("accept nullable big integer as String union") {

        val schema = Avro.default.schema(NullableTest.serializer())
        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigint_nullable.json"))
        schema shouldBe expected
    }
}) {
    @Serializable
    data class Test(val b: BigInteger)

    @Serializable
    data class NullableTest(val b: BigInteger?)
}