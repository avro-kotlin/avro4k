package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.serializer.GenericDataSerializer
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import kotlinx.serialization.builtins.nullable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

internal class GenericDataSerializationTest : StringSpec({
    val valueRecordSchema =
        SchemaBuilder.record("value").fields()
            .name("field").type().booleanType().noDefault()
            .endRecord()
    val valueFixedSchema = Schema.createFixed("fixed", null, null, 2)
    val valueEnumSchema = Schema.createEnum("enum", null, null, listOf("A", "B", "C"))
    val types =
        listOf(
            Schema.create(Schema.Type.BOOLEAN) to true,
            Schema.create(Schema.Type.INT) to 42,
            Schema.create(Schema.Type.LONG) to 42L,
            Schema.create(Schema.Type.FLOAT) to 42.0f,
            Schema.create(Schema.Type.DOUBLE) to 42.0,
            Schema.create(Schema.Type.STRING) to "hello",
            valueFixedSchema to GenericData.Fixed(valueFixedSchema, byteArrayOf(1, 2)),
            valueEnumSchema to GenericData.EnumSymbol(valueEnumSchema, "B"),
            Schema.createUnion(listOf(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.BOOLEAN))) to 17,
            Schema.createMap(Schema.create(Schema.Type.STRING)) to mapOf("key" to "value"),
            Schema.createArray(Schema.create(Schema.Type.INT)) to listOf(1, 2, 3),
            Schema.createArray(Schema.create(Schema.Type.INT)) to setOf(1, 2, 3),
            valueRecordSchema to GenericData.Record(valueRecordSchema).also { it.put(0, false) }
        )

    types.forEach { (schema, value) ->
        "scalar type ${schema.type}: basic serialization" {
            testEncodeDecodeGeneric(schema, value)
        }
        "scalar type ${schema.type}: nullable serialization" {
            testEncodeDecodeGeneric(schema.nullable, value)
            testEncodeDecodeGeneric(schema.nullable, null)
        }
        "scalar type ${schema.type} in record" {
            val record =
                SchemaBuilder.record("theRecord").fields()
                    .name("field").type(schema).noDefault()
                    .endRecord()
            testEncodeDecodeGeneric(record, GenericData.Record(record).also { it.put(0, value) })
        }
        "scalar type ${schema.type} in map" {
            val map = SchemaBuilder.map().values(schema)
            testEncodeDecodeGeneric(map, mapOf("key" to value))
        }
        "scalar type ${schema.type} in array" {
            val array = SchemaBuilder.array().items(schema)
            testEncodeDecodeGeneric(array, GenericData.Array<Any>(1, array).also { it.add(value) })
            testEncodeDecodeGeneric(array, listOf(value))
            testEncodeDecodeGeneric(array, arrayOf(value), decoded = listOf(value), expectedBytes = encodeToBytesUsingApacheLib(array, listOf(value)))
            testEncodeDecodeGeneric(array, setOf(value))
        }
    }

    val byteArray = byteArrayOf(1, 2, 3)
    val bytesSchema = Schema.create(Schema.Type.BYTES)
    "scalar type BYTES: basic serialization" {
        testEncodeDecodeGeneric(
            bytesSchema,
            byteArray,
            expectedBytes = encodeToBytesUsingApacheLib(bytesSchema, byteArray.asBuffer())
        )
    }
    "scalar type BYTES: nullable serialization" {
        val nullableSchema = bytesSchema.nullable
        testEncodeDecodeGeneric(nullableSchema, byteArray, expectedBytes = encodeToBytesUsingApacheLib(nullableSchema, byteArray.asBuffer()))
        testEncodeDecodeGeneric(nullableSchema, null)
    }
    "scalar type BYTES in record" {
        val record =
            SchemaBuilder.record("theRecord").fields()
                .name("field").type().bytesType().noDefault()
                .endRecord()
        val toEncode = GenericData.Record(record).also { it.put(0, byteArray) }
        val expectedBytes = encodeToBytesUsingApacheLib(record, GenericData.Record(record).also { it.put(0, byteArray.asBuffer()) })
        Avro.encodeToByteArray<Any?>(record, GenericDataSerializer.nullable, toEncode) shouldBe expectedBytes

        // records having byte-arrays are not comparable
        val decoded = Avro.decodeFromByteArray(record, GenericDataSerializer.nullable, expectedBytes)
        decoded should beInstanceOf<GenericRecord>()
        (decoded as GenericRecord).schema shouldBe record
        decoded[0] shouldBe byteArray
    }
    "scalar type BYTES in map" {
        val map = SchemaBuilder.map().values(bytesSchema)
        testEncodeDecodeGeneric(map, mapOf("key" to byteArray), expectedBytes = encodeToBytesUsingApacheLib(map, mapOf("key" to byteArray.asBuffer())))
    }
    "scalar type BYTES in array" {
        val array = SchemaBuilder.array().items(bytesSchema)
        testEncodeDecodeGeneric(array, listOf(byteArray), expectedBytes = encodeToBytesUsingApacheLib(array, listOf(byteArray.asBuffer())))
        testEncodeDecodeGeneric(array, setOf(byteArray), expectedBytes = encodeToBytesUsingApacheLib(array, setOf(byteArray.asBuffer())))
        testEncodeDecodeGeneric(array, arrayOf(byteArray), decoded = listOf(byteArray), expectedBytes = encodeToBytesUsingApacheLib(array, listOf(byteArray.asBuffer())))
        testEncodeDecodeGeneric(
            array,
            GenericData.Array<Any>(array, listOf(byteArray)),
            decoded = listOf(byteArray),
            expectedBytes = encodeToBytesUsingApacheLib(array, GenericData.Array<Any>(array, listOf(byteArray.asBuffer())))
        )
    }
})

private fun encodeToBytesUsingApacheLib(
    schema: Schema,
    toEncode: Any?,
): ByteArray {
    return ByteArrayOutputStream().use {
        GenericData.get().createDatumWriter(schema).write(toEncode, EncoderFactory.get().directBinaryEncoder(it, null))
        it.toByteArray()
    }
}

private fun testEncodeDecodeGeneric(
    schema: Schema,
    toEncode: Any?,
    decoded: Any? = toEncode,
    expectedBytes: ByteArray = encodeToBytesUsingApacheLib(schema, toEncode),
) {
    Avro.encodeToByteArray(schema, GenericDataSerializer.nullable, toEncode) shouldBe expectedBytes
    Avro.decodeFromByteArray(schema, GenericDataSerializer.nullable, expectedBytes) shouldBe decoded
}

private fun ByteArray.asBuffer() = ByteBuffer.wrap(this)