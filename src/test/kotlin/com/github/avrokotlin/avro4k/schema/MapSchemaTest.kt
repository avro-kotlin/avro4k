package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.SomeEnum
import com.github.avrokotlin.avro4k.WrappedBoolean
import com.github.avrokotlin.avro4k.WrappedByte
import com.github.avrokotlin.avro4k.WrappedChar
import com.github.avrokotlin.avro4k.WrappedDouble
import com.github.avrokotlin.avro4k.WrappedEnum
import com.github.avrokotlin.avro4k.WrappedFloat
import com.github.avrokotlin.avro4k.WrappedInt
import com.github.avrokotlin.avro4k.WrappedLong
import com.github.avrokotlin.avro4k.WrappedShort
import com.github.avrokotlin.avro4k.WrappedString
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.serializersModuleOf
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import kotlin.io.path.Path

@OptIn(InternalSerializationApi::class)
class MapSchemaTest : FunSpec({
    test("generate map type for a Map of ints") {
        val map = mapOf("a" to 1, "b" to 20, "c" to 5)
        AvroAssertions.assertThat(StringIntTest(map))
            .isEncodedAs(record(map))
        AvroAssertions.assertThat<StringIntTest>()
            .generatesSchema(Path("/map_int.json"))
    }

    test("generate map type for a Map of records") {
        AvroAssertions.assertThat<StringNestedTest>()
            .generatesSchema(Path("/map_record.json"))
        AvroAssertions.assertThat(StringNestedTest(mapOf("a" to Nested("goo"))))
            .isEncodedAs(record(mapOf("a" to record("goo"))))
    }

    test("generate map type for map of nullable booleans") {
        val map = mapOf("a" to null, "b" to true, "c" to false)
        AvroAssertions.assertThat<StringBooleanTest>()
            .generatesSchema(Path("/map_boolean_null.json"))
        AvroAssertions.assertThat(StringBooleanTest(map))
            .isEncodedAs(record(map))
    }

    test("support maps of sets of records") {
        AvroAssertions.assertThat<StringSetNestedTest>()
            .generatesSchema(Path("/map_set_nested.json"))
        AvroAssertions.assertThat(StringSetNestedTest(mapOf("a" to setOf(Nested("goo")))))
            .isEncodedAs(
                record(mapOf("a" to listOf(record("goo"))))
            )
    }

    test("support array of maps") {
        AvroAssertions.assertThat<ArrayTest>()
            .generatesSchema(Path("/array_of_maps.json"))
        AvroAssertions.assertThat(ArrayTest(arrayOf(mapOf("a" to "b"))))
            .isEncodedAs(record(listOf(mapOf("a" to "b"))))
    }

    test("support array of maps where the key is a value class") {
        AvroAssertions.assertThat<WrappedStringArrayTest>()
            .generatesSchema(Path("/array_of_maps.json"))
        AvroAssertions.assertThat(WrappedStringArrayTest(arrayOf(mapOf(WrappedString("a") to "b"))))
            .isEncodedAs(record(listOf(mapOf("a" to "b"))))
    }

    test("support lists of maps") {
        AvroAssertions.assertThat<ListTest>()
            .generatesSchema(Path("/list_of_maps.json"))
        AvroAssertions.assertThat(ListTest(listOf(mapOf("a" to "b"))))
            .isEncodedAs(record(listOf(mapOf("a" to "b"))))
    }

    test("support sets of maps") {
        AvroAssertions.assertThat<SetTest>()
            .generatesSchema(Path("/set_of_maps.json"))
        AvroAssertions.assertThat(SetTest(setOf(mapOf("a" to "b"))))
            .isEncodedAs(record(listOf(mapOf("a" to "b"))))
    }

    test("support data class of list of data class with maps") {
        AvroAssertions.assertThat<List2Test>()
            .generatesSchema(Path("/class_of_list_of_maps.json"))
        AvroAssertions.assertThat(List2Test(listOf(mapOf("a" to "b"))))
            .isEncodedAs(
                record(listOf(mapOf("a" to "b")))
            )
    }

    test("support maps with contextual keys") {
        AvroAssertions.assertThat<ContextualKeyTests>()
            .withConfig {
                serializersModule = serializersModuleOf(NonSerializableKeyKSerializer)
            }
            .generatesSchema(
                SchemaBuilder.record("ContextualKeyTests").fields()
                    .name("map").type(Schema.createMap(Schema.create(Schema.Type.INT))).noDefault()
                    .endRecord()
            )
        AvroAssertions.assertThat(ContextualKeyTests(mapOf(NonSerializableKey("a") to 12)))
            .withConfig {
                serializersModule = serializersModuleOf(NonSerializableKeyKSerializer)
            }
            .isEncodedAs(record(mapOf("a" to 12)))
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
            AvroAssertions.assertThat(GenericMapForTests.serializer(keyValue::class.serializer()))
                .generatesSchema(Path("/map_string.json"))
            AvroAssertions.assertThat(GenericMapForTests(mapOf(keyValue to "something")), GenericMapForTests.serializer(keyValue::class.serializer()))
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
        WrappedEnum(SomeEnum.B) to "B",
        WrappedChar('a') to "a"
    ).forEach { (keyValue, avroValue) ->
        test("handle string-able key type inside a value class: ${keyValue::class.simpleName}") {
            AvroAssertions.assertThat(GenericMapForTests.serializer(keyValue::class.serializer()))
                .generatesSchema(Path("/map_string.json"))
            AvroAssertions.assertThat(GenericMapForTests(mapOf(keyValue to "something")), GenericMapForTests.serializer(keyValue::class.serializer()))
                .isEncodedAs(record(mapOf(avroValue to "something")))
        }
    }

    test("should fail on nullable key") {
        shouldThrow<AvroSchemaGenerationException> {
            Avro.schema(GenericMapForTests.serializer(String.serializer().nullable))
        }
    }
    test("should fail on non-stringable key type: record") {
        shouldThrow<AvroSchemaGenerationException> {
            Avro.schema(GenericMapForTests.serializer(DataRecord.serializer()))
        }
    }
    test("should fail on non-stringable key type: map") {
        shouldThrow<AvroSchemaGenerationException> {
            Avro.schema(GenericMapForTests.serializer(MapSerializer(String.serializer(), String.serializer())))
        }
    }
    test("should fail on non-stringable key type: array") {
        shouldThrow<AvroSchemaGenerationException> {
            Avro.schema(GenericMapForTests.serializer(ListSerializer(String.serializer())))
        }
    }
    test("should fail on non-stringable key type: bytes") {
        shouldThrow<AvroSchemaGenerationException> {
            Avro.schema(GenericMapForTests.serializer(ListSerializer(Byte.serializer())))
        }
    }
}) {
    @Serializable
    @SerialName("mapStringStringTest")
    private data class GenericMapForTests<K>(val map: Map<K, String>)

    @Serializable
    @SerialName("ContextualKeyTests")
    private data class ContextualKeyTests(val map: Map<@Contextual NonSerializableKey, Int>)

    private data class NonSerializableKey(val key: String)

    private object NonSerializableKeyKSerializer : KSerializer<NonSerializableKey> {
        @OptIn(InternalSerializationApi::class)
        override val descriptor = buildSerialDescriptor("NonSerializableKey", PrimitiveKind.STRING)

        override fun deserialize(decoder: Decoder) = NonSerializableKey(decoder.decodeString())

        override fun serialize(
            encoder: Encoder,
            value: NonSerializableKey,
        ) {
            encoder.encodeString(value.key)
        }
    }

    @Serializable
    private data class DataRecord(val field: Int)

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
    private data class ArrayTest(val array: Array<Map<String, String>>) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as ArrayTest

            return array.contentEquals(other.array)
        }

        override fun hashCode(): Int {
            return array.contentHashCode()
        }
    }

    @Serializable
    @SerialName("arrayOfMapStringString")
    private data class WrappedStringArrayTest(val array: Array<Map<WrappedString, String>>) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as WrappedStringArrayTest

            return array.contentEquals(other.array)
        }

        override fun hashCode(): Int {
            return array.contentHashCode()
        }
    }

    @Serializable
    private data class ListTest(val list: List<Map<String, String>>)

    @Serializable
    private data class SetTest(val set: Set<Map<String, String>>)

    @Serializable
    private data class List2Test(val ship: List<Map<String, String>>)
}