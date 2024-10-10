package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.basicScalarEncodeDecodeTests
import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.testSerializationTypeCompatibility
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.LogicalType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

internal class PrimitiveEncodingTest : StringSpec({
    // boolean can be encoded to boolean or string
    basicScalarEncodeDecodeTests(true, Schema.create(Schema.Type.BOOLEAN))
    testSerializationTypeCompatibility(true, "true", Schema.create(Schema.Type.STRING))
    basicScalarEncodeDecodeTests(false, Schema.create(Schema.Type.BOOLEAN))
    testSerializationTypeCompatibility(false, "false", Schema.create(Schema.Type.STRING))

    // byte can be encoded to int, long or string
    basicScalarEncodeDecodeTests(1.toByte(), Schema.create(Schema.Type.INT), apacheCompatibleValue = 1)
    testSerializationTypeCompatibility(1.toByte(), "1", Schema.create(Schema.Type.STRING))
    testSerializationTypeCompatibility(1.toByte(), 1L, Schema.create(Schema.Type.LONG))

    // short can be encoded to int, long or string
    basicScalarEncodeDecodeTests(2.toShort(), Schema.create(Schema.Type.INT), apacheCompatibleValue = 2)
    testSerializationTypeCompatibility(2.toShort(), "2", Schema.create(Schema.Type.STRING))
    testSerializationTypeCompatibility(2.toShort(), 2L, Schema.create(Schema.Type.LONG))

    // int can be encoded to int, long or string
    basicScalarEncodeDecodeTests(3, Schema.create(Schema.Type.INT))
    testSerializationTypeCompatibility(3, "3", Schema.create(Schema.Type.STRING))
    testSerializationTypeCompatibility(3, 3L, Schema.create(Schema.Type.LONG))

    // long can be encoded to int, long or string
    basicScalarEncodeDecodeTests(4L, Schema.create(Schema.Type.LONG))
    testSerializationTypeCompatibility(4L, "4", Schema.create(Schema.Type.STRING))
    testSerializationTypeCompatibility(4L, 4, Schema.create(Schema.Type.INT))

    // float can be encoded to double, float or string
    basicScalarEncodeDecodeTests(5.0F, Schema.create(Schema.Type.FLOAT))
    testSerializationTypeCompatibility(5.0F, "5.0", Schema.create(Schema.Type.STRING))
    testSerializationTypeCompatibility(5.0F, 5.0, Schema.create(Schema.Type.DOUBLE))

    // double can be encoded to double, float or string
    basicScalarEncodeDecodeTests(6.0, Schema.create(Schema.Type.DOUBLE))
    testSerializationTypeCompatibility(6.0, "6.0", Schema.create(Schema.Type.STRING))
    testSerializationTypeCompatibility(6.0, 6.0F, Schema.create(Schema.Type.FLOAT))

    // char can be encoded to int or string
    basicScalarEncodeDecodeTests('A', Schema.create(Schema.Type.INT).copy(logicalType = LogicalType("char")), apacheCompatibleValue = 'A'.code)
    testSerializationTypeCompatibility('A', "A", Schema.create(Schema.Type.STRING))

    // bytes can be encoded to bytes, string, or fixed
    val bytesValue = "test".encodeToByteArray()
    basicScalarEncodeDecodeTests(bytesValue, Schema.create(Schema.Type.BYTES), apacheCompatibleValue = ByteBuffer.wrap(bytesValue))
    testSerializationTypeCompatibility(bytesValue, "test", Schema.create(Schema.Type.STRING))
    val fixedBytesSchema = Schema.createFixed("fixed", null, null, bytesValue.size)
    testSerializationTypeCompatibility(bytesValue, GenericData.Fixed(fixedBytesSchema, bytesValue), fixedBytesSchema)

    // fixed can be encoded to bytes, string, or fixed
    val fixedValue = FixedValue("fixed".encodeToByteArray())
    val fixedSchema = Schema.createFixed("fixed", null, null, fixedValue.value.size)
    // not able to directly serialize a fixed, so we need to wrap it in a value class to indicates it's a fixed type
    testSerializationTypeCompatibility(fixedValue, ByteBuffer.wrap(fixedValue.value), Schema.create(Schema.Type.BYTES))
    testSerializationTypeCompatibility(fixedValue, "fixed", Schema.create(Schema.Type.STRING))
    testSerializationTypeCompatibility(fixedValue, GenericData.Fixed(fixedSchema, fixedValue.value), fixedSchema)

    // string can be encoded to bytes, string, or fixed
    val stringValue = "the string content"
    basicScalarEncodeDecodeTests(stringValue, Schema.create(Schema.Type.STRING))
    testSerializationTypeCompatibility(stringValue, ByteBuffer.wrap(stringValue.encodeToByteArray()), Schema.create(Schema.Type.BYTES))
    val fixedStringSchema = Schema.createFixed("fixed", null, null, stringValue.length)
    testSerializationTypeCompatibility(stringValue, GenericData.Fixed(fixedStringSchema, stringValue.encodeToByteArray()), fixedStringSchema)

    testSerializationTypeCompatibility("true", true, Schema.create(Schema.Type.BOOLEAN))
    testSerializationTypeCompatibility("false", false, Schema.create(Schema.Type.BOOLEAN))
    testSerializationTypeCompatibility("23", 23, Schema.create(Schema.Type.INT))
    testSerializationTypeCompatibility("55", 55L, Schema.create(Schema.Type.LONG))
    testSerializationTypeCompatibility("5.3", 5.3F, Schema.create(Schema.Type.FLOAT))
    testSerializationTypeCompatibility("5.3", 5.3, Schema.create(Schema.Type.DOUBLE))
}) {
    @JvmInline
    @Serializable
    private value class FixedValue(
        @AvroFixed(5) val value: ByteArray,
    )
}