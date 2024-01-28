@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol

enum class Cream {
    Bruce,
    Baker,
    Clapton,
}

enum class BBM {
    Bruce,
    Baker,
    Moore,
}

class EnumIoTest : StringSpec({

    "read / write enums" {

        writeRead(EnumTest(Cream.Bruce, BBM.Moore), EnumTest.serializer())
        writeRead(EnumTest(Cream.Bruce, BBM.Moore), EnumTest.serializer()) {
            (it["a"] as GenericEnumSymbol<*>).toString() shouldBe "Bruce"
            (it["b"] as GenericEnumSymbol<*>).toString() shouldBe "Moore"
        }
    }

    "read / write list of enums" {

        writeRead(EnumListTest(listOf(Cream.Bruce, Cream.Clapton)), EnumListTest.serializer())
        writeRead(EnumListTest(listOf(Cream.Bruce, Cream.Clapton)), EnumListTest.serializer()) { record ->
            (record["a"] as List<*>).map { it.toString() } shouldBe listOf("Bruce", "Clapton")
        }
    }

    "read / write nullable enums" {

        writeRead(NullableEnumTest(null), NullableEnumTest.serializer())
        writeRead(NullableEnumTest(Cream.Bruce), NullableEnumTest.serializer())
        writeRead(NullableEnumTest(Cream.Bruce), NullableEnumTest.serializer()) {
            (it["a"] as GenericData.EnumSymbol).toString() shouldBe "Bruce"
        }
    }
}) {
    @Serializable
    data class EnumTest(val a: Cream, val b: BBM)

    @Serializable
    data class EnumListTest(val a: List<Cream>)

    @Serializable
    data class NullableEnumTest(val a: Cream?)
}