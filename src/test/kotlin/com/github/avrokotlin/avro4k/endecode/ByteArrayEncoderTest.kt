package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

class ByteArrayEncoderTest : FunSpec({
    includeForEveryEncoder { byteArrayEncoderTests(it) }
})

fun byteArrayEncoderTests(encoderToTest: EnDecoder): TestFactory {
    return stringSpec {
        @Serializable
        data class ByteArrayTest(val z: ByteArray)

        fun avroByteArray(vararg bytes: Byte) = ByteBuffer.wrap(bytes)
        "encode/decode ByteArray" {
            encoderToTest.testEncodeDecode(
                ByteArrayTest(byteArrayOf(1, 4, 9)),
                record(avroByteArray(1, 4, 9))
            )
        }
        "encode/decode List<Byte>" {
            @Serializable
            data class ListByteTest(val z: List<Byte>)
            encoderToTest.testEncodeDecode(ListByteTest(listOf(1, 4, 9)), record(avroByteArray(1, 4, 9)))
        }

        "encode/decode Array<Byte> to ByteBuffer" {
            @Serializable
            data class ArrayByteTest(val z: Array<Byte>)
            encoderToTest.testEncodeDecode(ArrayByteTest(arrayOf(1, 4, 9)), record(avroByteArray(1, 4, 9)))
        }

        "encode/decode ByteArray as FIXED when schema is Type.Fixed" {
            val fixedSchema = Schema.createFixed("ByteArray", null, null, 8)
            val schema =
                SchemaBuilder.record("ByteArrayTest").fields().name("z").type(fixedSchema).noDefault().endRecord()
            val unpaddedByteArray = byteArrayOf(1, 4, 9)
            val paddedByteArray = byteArrayOf(0, 0, 0, 0, 0, 1, 4, 9)
            val encoded =
                encoderToTest.testEncodeIsEqual(
                    value = ByteArrayTest(unpaddedByteArray),
                    shouldMatch = record(GenericData.Fixed(fixedSchema, paddedByteArray)),
                    schema = schema
                )
            encoderToTest.testDecodeIsEqual(
                byteArray = encoded,
                value = ByteArrayTest(paddedByteArray),
                readSchema = schema
            )
        }
    }
}