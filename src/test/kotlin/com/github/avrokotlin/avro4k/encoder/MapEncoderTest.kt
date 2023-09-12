package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.throwables.shouldThrowWithMessage
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Contextual
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.serializersModuleOf
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class MapEncoderTest : StringSpec({

    "encode a Map with non-string key" should {
        "encode int key should fail" {
            val schema = SchemaBuilder.map().values(Schema.create(Schema.Type.STRING))
            shouldThrow<SerializationException> {
                Avro.default.encodeToGenericData(mapOf(12 to "a", 56 to "b", 25 to "c"), schema)
            }
        }
        "encode value class key as string" {
            val schema = SchemaBuilder.map().values(Schema.create(Schema.Type.STRING))
            val encoded = Avro.default.encodeToGenericData(
                mapOf(StringKey("a") to "1", StringKey("b") to "2", StringKey("c") to "3"),
                schema
            )
            encoded shouldBe mapOf(Utf8("a") to Utf8("1"), Utf8("b") to Utf8("2"), Utf8("c") to Utf8("3"))
        }
        "encode contextual key as string" {
            val schema = SchemaBuilder.record("ContextMapKey").fields().name("map").type(SchemaBuilder.map().values(Schema.create(Schema.Type.STRING))).noDefault().endRecord()
            val encoded = Avro(serializersModuleOf(NonSerializableClassSerializer)).encodeToGenericData(
                ContextMapKey(mapOf(NonSerializableClass("key") to "value")),
                schema
            )
            encoded shouldBeContentOf ListRecord(schema, mapOf(Utf8("key") to Utf8("value")))
        }
        "encode structure key should fail" {
            val schema = SchemaBuilder.map().values(Schema.create(Schema.Type.STRING))
            shouldThrowWithMessage<SerializationException>("Cannot encode structure for map keys: only allowed to encode primitive types for map keys") {
                Avro.default.encodeToGenericData(mapOf(Foo("", true) to "data"), schema)
            }
        }
        "encode collection key should fail" {
            val schema = SchemaBuilder.map().values(Schema.create(Schema.Type.STRING))
            shouldThrowWithMessage<SerializationException>("Cannot encode collection for map keys: only allowed to encode primitive types for map keys") {
                Avro.default.encodeToGenericData(mapOf(listOf("test") to "data"), schema)
            }
        }
    }

    "encode a Map<String, Boolean>" {

        val schema = Avro.default.schema(StringBooleanTest.serializer())
        val record = Avro.default.encodeToGenericData(StringBooleanTest(mapOf("a" to true, "b" to false, "c" to true)))
        record shouldBeContentOf ListRecord(schema, mapOf("a" to true, "b" to false, "c" to true))
    }

    "encode a Map<String, String>" {

        val schema = Avro.default.schema(StringStringTest.serializer())
        val record = Avro.default.encodeToGenericData(StringStringTest(mapOf("a" to "x", "b" to "y", "c" to "z")))
        record shouldBeContentOf ListRecord(schema, mapOf("a" to "x", "b" to "y", "c" to "z"))
    }

    "encode a Map<String, ByteArray>" {

        val schema = Avro.default.schema(StringByteArrayTest.serializer())
        val record = Avro.default.encodeToGenericData(StringByteArrayTest(mapOf(
            "a" to "x".toByteArray(),
            "b" to "y".toByteArray(),
            "c" to "z".toByteArray()
        )))
        record shouldBeContentOf ListRecord(schema, mapOf(
            "a" to ByteBuffer.wrap("x".toByteArray()),
            "b" to ByteBuffer.wrap("y".toByteArray()),
            "c" to ByteBuffer.wrap("z".toByteArray())
        ))
    }

    "encode a Map of records" {

        val schema = Avro.default.schema(StringFooTest.serializer())
        val fooSchema = Avro.default.schema(Foo.serializer())

        val record = Avro.default.encodeToGenericData(StringFooTest(mapOf("a" to Foo("x", true), "b" to Foo("y", false))))
        val xRecord = ListRecord(fooSchema, Utf8("x"), true)
        val yRecord = ListRecord(fooSchema, Utf8("y"), false)

        record shouldBeContentOf ListRecord(schema, mapOf("a" to xRecord, "b" to yRecord))
    }
}) {
    @JvmInline
    @Serializable
    value class StringKey(val value: String)

    @Serializable
    @SerialName("ContextMapKey")
    data class ContextMapKey(val map: Map<@Contextual NonSerializableClass, String>)

    data class NonSerializableClass(val key: String)

    object NonSerializableClassSerializer : KSerializer<NonSerializableClass> {
        override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("NonSerializableClass", PrimitiveKind.STRING)
        override fun deserialize(decoder: Decoder) = TODO("Useless for tests")
        override fun serialize(encoder: Encoder, value: NonSerializableClass) {
            encoder.encodeString(value.key)
        }
    }

    @Serializable
    data class StringBooleanTest(val a: Map<String, Boolean>)

    @Serializable
    data class StringStringTest(val a: Map<String, String>)

    @Serializable
    data class StringByteArrayTest(val a: Map<String, ByteArray>)

    @Serializable
    data class Foo(val a: String, val b: Boolean)

    @Serializable
    data class StringFooTest(val a: Map<String, Foo>)
}
