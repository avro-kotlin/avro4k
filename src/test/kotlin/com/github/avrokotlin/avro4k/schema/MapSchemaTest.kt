package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroSerializationAssertThat.Companion.assertThat
import com.github.avrokotlin.avro4k.SomeEnum
import com.github.avrokotlin.avro4k.WrappedBoolean
import com.github.avrokotlin.avro4k.WrappedByte
import com.github.avrokotlin.avro4k.WrappedChar
import com.github.avrokotlin.avro4k.WrappedDouble
import com.github.avrokotlin.avro4k.WrappedFloat
import com.github.avrokotlin.avro4k.WrappedInt
import com.github.avrokotlin.avro4k.WrappedLong
import com.github.avrokotlin.avro4k.WrappedShort
import com.github.avrokotlin.avro4k.WrappedString
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import kotlin.io.path.Path

@OptIn(InternalSerializationApi::class)
class MapSchemaTest : FunSpec({
    test("generate map type for a Map of ints") {
        val map = mapOf("a" to 1, "b" to 20, "c" to 5)
        assertThat(StringIntTest(map))
            .generatesSchema(Path("/map_int.json"))
            .isEncodedAs(record(map))
    }

    test("generate map type for a Map of records") {
        val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/map_record.json"))
        val schema = Avro.default.schema(StringNestedTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("generate map type for map of nullable booleans") {
        val map = mapOf("a" to null, "b" to true, "c" to false)
        assertThat(StringBooleanTest(map))
            .generatesSchema(Path("/map_boolean_null.json"))
            .isEncodedAs(record(map))
    }

    test("support maps of sets of records") {
        val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/map_set_nested.json"))
        val schema = Avro.default.schema(StringSetNestedTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("support array of maps") {
        val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/array_of_maps.json"))
        val schema = Avro.default.schema(ArrayTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("support array of maps where the key is a value class") {
        val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/array_of_maps.json"))
        val schema = Avro.default.schema(WrappedStringArrayTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("support lists of maps") {
        val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/list_of_maps.json"))
        val schema = Avro.default.schema(ListTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("support sets of maps") {
        val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/set_of_maps.json"))
        val schema = Avro.default.schema(SetTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("support data class of list of data class with maps") {
        val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/class_of_list_of_maps.json"))
        val schema = Avro.default.schema(List2Test.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    listOf(
        true,
        1.toByte(),
        1.toShort(),
        1,
        1.toLong(),
        1.toFloat(),
        1.toDouble(),
        1.toString(),
        'a',
        SomeEnum.B
    ).forEach { keyValue ->
        test("handle string-able key type: ${keyValue::class.simpleName}") {
            assertThat(GenericMapForTests(mapOf(keyValue to "something")), GenericMapForTests.serializer(keyValue::class.serializer()))
                .generatesSchema(Path("/map_string.json"))
                .isEncodedAs(record(mapOf(keyValue.toString() to "something")))
        }
    }

    listOf(
        WrappedBoolean(true) to true,
        WrappedInt(1) to 1,
        WrappedByte(1.toByte()) to 1.toByte(),
        WrappedShort(1.toShort()) to 1.toShort(),
        WrappedLong(1.toLong()) to 1.toLong(),
        WrappedFloat(1.toFloat()) to 1.toFloat(),
        WrappedDouble(1.toDouble()) to 1.toDouble(),
        WrappedString(1.toString()) to 1.toString(),
        // todo add WrappedEnum::class, only works with kotlin 1.9.20+: https://youtrack.jetbrains.com/issue/KT-57647/Serialization-IllegalAccessError-Update-to-static-final-field-caused-by-serializable-value-class
        WrappedChar('a') to "a"
    ).forEach { (keyValue, avroValue) ->
        test("handle string-able key type inside a value class: ${keyValue::class.simpleName}") {
            assertThat(GenericMapForTests(mapOf(keyValue to "something")), GenericMapForTests.serializer(keyValue::class.serializer()))
                .generatesSchema(Path("/map_string.json"))
                .isEncodedAs(record(mapOf(avroValue to "something")))
        }
    }
}) {
    @Serializable
    @SerialName("mapStringStringTest")
    private data class GenericMapForTests<K>(val map: Map<K, String>)

    @Serializable
    private data class StringIntTest(val map: Map<String, Int>)

    @Serializable
    private data class Nested(val goo: String)

    @Serializable
    private data class StringNestedTest(val map: Map<String, Nested>)

    @Serializable
    private data class StringBooleanTest(val map: Map<String, Boolean?>)

    @Serializable
    private data class StringSetNestedTest(val map: Map<String, Set<Nested>>)

    @Serializable
    @SerialName("arrayOfMapStringString")
    private data class ArrayTest(val array: Array<Map<String, String>>)

    @Serializable
    @SerialName("arrayOfMapStringString")
    private data class WrappedStringArrayTest(val array: Array<Map<WrappedString, String>>)

    @Serializable
    private data class ListTest(val list: List<Map<String, String>>)

    @Serializable
    private data class SetTest(val set: Set<Map<String, String>>)

    @Serializable
    private data class List2Test(val ship: List<Map<String, String>>)
}