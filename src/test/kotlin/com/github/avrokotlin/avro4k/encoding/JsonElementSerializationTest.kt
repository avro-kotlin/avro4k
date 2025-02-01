package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.serializer.JsonElementSerializer
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

internal class JsonElementSerializationTest : StringSpec({
    val valueRecordSchema =
        SchemaBuilder.record("value").fields()
            .name("field").type().booleanType().noDefault()
            .endRecord()
    val valueFixedSchema = Schema.createFixed("fixed", null, null, 2)
    val valueEnumSchema = Schema.createEnum("enum", null, null, listOf("A", "B", "C"))
    val types =
        listOf(
            DataForTests(Schema.create(Schema.Type.BOOLEAN), JsonPrimitive(true), true),
            DataForTests(Schema.create(Schema.Type.NULL), JsonNull, null),
            DataForTests(Schema.create(Schema.Type.INT), JsonPrimitive(42), 42),
            DataForTests(Schema.create(Schema.Type.LONG), JsonPrimitive(42L), 42L),
            DataForTests(Schema.create(Schema.Type.FLOAT), JsonPrimitive(42.0f), 42.0f),
            DataForTests(Schema.create(Schema.Type.DOUBLE), JsonPrimitive(42.0), 42.0),
            DataForTests(Schema.create(Schema.Type.STRING), JsonPrimitive("hello"), "hello"),
            DataForTests(valueFixedSchema, byteArrayOf(1, 2).toBase64JsonElement(), GenericData.Fixed(valueFixedSchema, byteArrayOf(1, 2))),
            DataForTests(valueEnumSchema, JsonPrimitive("B"), GenericData.get().createEnum("B", valueEnumSchema)),
            DataForTests(Schema.createMap(Schema.create(Schema.Type.STRING)), JsonObject(mapOf("key" to JsonPrimitive("value"))), mapOf("key" to "value")),
            DataForTests(Schema.createArray(Schema.create(Schema.Type.INT)), JsonArray(listOf(JsonPrimitive(1), JsonPrimitive(2), JsonPrimitive(3))), listOf(1, 2, 3)),
            DataForTests(valueRecordSchema, JsonObject(mapOf("field" to JsonPrimitive(false))), GenericData.Record(valueRecordSchema).also { it.put(0, false) })
        )

    types.forEach { (schema, value, genericValue) ->
        "scalar type ${schema.type}: basic serialization" {
            testEncodeDecodeGeneric(schema, value, expectedBytes = encodeToBytesUsingApacheLib(schema, genericValue))
        }
        "scalar type ${schema.type}: nullable serialization" {
            val nullableSchema = schema.nullable
            testEncodeDecodeGeneric(nullableSchema, value, expectedBytes = encodeToBytesUsingApacheLib(nullableSchema, genericValue))
            testEncodeDecodeGeneric(nullableSchema, JsonNull, expectedBytes = encodeToBytesUsingApacheLib(nullableSchema, null))
        }
        "scalar type ${schema.type} in record" {
            val record =
                SchemaBuilder.record("theRecord").fields()
                    .name("field").type(schema).noDefault()
                    .endRecord()
            testEncodeDecodeGeneric(
                record,
                JsonObject(mapOf("field" to value)),
                expectedBytes = encodeToBytesUsingApacheLib(record, GenericData.Record(record).also { it.put(0, genericValue) })
            )
        }
        "scalar type ${schema.type} in map" {
            val map = SchemaBuilder.map().values(schema)
            testEncodeDecodeGeneric(map, JsonObject(mapOf("key" to value)), expectedBytes = encodeToBytesUsingApacheLib(map, mapOf("key" to genericValue)))
        }
        "scalar type ${schema.type} in array" {
            val array = SchemaBuilder.array().items(schema)
            testEncodeDecodeGeneric(array, JsonArray(listOf(value)), expectedBytes = encodeToBytesUsingApacheLib(array, listOf(genericValue)))
        }
    }

    val byteArray = byteArrayOf(1, 2, 3)
    val bytesSchema = Schema.create(Schema.Type.BYTES)
    "scalar type BYTES: basic serialization" {
        testEncodeDecodeGeneric(
            bytesSchema,
            byteArray.toBase64JsonElement(),
            expectedBytes = encodeToBytesUsingApacheLib(bytesSchema, byteArray.asBuffer())
        )
    }
    "scalar type BYTES: nullable serialization" {
        val nullableSchema = bytesSchema.nullable
        testEncodeDecodeGeneric(
            nullableSchema,
            byteArray.toBase64JsonElement(),
            expectedBytes = encodeToBytesUsingApacheLib(nullableSchema, byteArray.asBuffer())
        )
        testEncodeDecodeGeneric(nullableSchema, JsonNull, expectedBytes = encodeToBytesUsingApacheLib(nullableSchema, null))
    }
    "scalar type BYTES in record" {
        val record =
            SchemaBuilder.record("theRecord").fields()
                .name("field").type().bytesType().noDefault()
                .endRecord()
        val toEncode = JsonObject(mapOf("field" to byteArray.toBase64JsonElement()))
        val expectedBytes =
            encodeToBytesUsingApacheLib(record, GenericData.Record(record).also { it.put(0, byteArray.asBuffer()) })
        Avro.encodeToByteArray<JsonElement?>(record, JsonElementSerializer.nullable, toEncode) shouldBe expectedBytes

        // records having byte-arrays are not comparable
        val decoded = Avro.decodeFromByteArray(record, JsonElementSerializer.nullable, expectedBytes)
        decoded should beInstanceOf<JsonObject>()
        decoded!!.jsonObject["field"] shouldBe byteArray.toBase64JsonElement()
    }
    "scalar type BYTES in map" {
        val map = SchemaBuilder.map().values(bytesSchema)
        testEncodeDecodeGeneric(
            map,
            JsonObject(mapOf("key" to byteArray.toBase64JsonElement())),
            expectedBytes = encodeToBytesUsingApacheLib(map, mapOf("key" to byteArray.asBuffer()))
        )
    }
    "scalar type BYTES in array" {
        val array = SchemaBuilder.array().items(bytesSchema)
        testEncodeDecodeGeneric(
            array,
            JsonArray(listOf(byteArray.toBase64JsonElement())),
            expectedBytes = encodeToBytesUsingApacheLib(array, listOf(byteArray.asBuffer()))
        )
    }
}) {
    private companion object {
        fun testEncodeDecodeGeneric(
            schema: Schema,
            toEncode: JsonElement,
            decoded: JsonElement? = toEncode,
            expectedBytes: ByteArray,
        ) {
            Avro.encodeToByteArray(schema, Avro.serializersModule.serializer(toEncode::class, emptyList(), false), toEncode) shouldBe expectedBytes
            Avro.decodeFromByteArray(schema, JsonElementSerializer, expectedBytes) shouldBe decoded
        }

        data class DataForTests(val schema: Schema, val value: JsonElement, val genericValue: Any?)
    }
}

@OptIn(ExperimentalEncodingApi::class)
private fun ByteArray.toBase64JsonElement() = JsonPrimitive(Base64.Mime.encode(this))