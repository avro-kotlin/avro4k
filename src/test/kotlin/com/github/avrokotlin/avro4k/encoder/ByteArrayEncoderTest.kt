package com.github.avrokotlin.avro4k.encoder

import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

fun byteArrayEncoderTests(encoderToTest: EncoderToTest): TestFactory {
    return funSpec {
        @Serializable
        data class ByteArrayTest(val z: ByteArray)

        fun avroByteArray(vararg bytes: Byte) =
            ByteBuffer.wrap(bytes)


        test("encode ByteArray") {
            encoderToTest.testEncodeDecode(
                ByteArrayTest(byteArrayOf(1, 4, 9)),
                record(avroByteArray(1, 4, 9))
            )
        }

        test("encode List<Byte>") {
            @Serializable
            data class ListByteTest(val z: List<Byte>)
            encoderToTest.testEncodeDecode(ListByteTest(listOf(1, 4, 9)), record(avroByteArray(1, 4, 9)))
        }

        test("encode Array<Byte> to ByteBuffer") {
            @Serializable
            data class ArrayByteTest(val z: Array<Byte>)
            encoderToTest.testEncodeDecode(ArrayByteTest(arrayOf(1, 4, 9)), record(avroByteArray(1, 4, 9)))
        }

        test("encode ByteArray as FIXED when schema is Type.Fixed") {
            val fixedSchema = Schema.createFixed("ByteArray", null, null, 8)
            val schema = SchemaBuilder.record("ByteArrayTest").fields()
                .name("z").type(fixedSchema).noDefault()
                .endRecord()
            encoderToTest.testEncodeDecode(
                value = ByteArrayTest(byteArrayOf(1, 4, 9)),
                shouldMatch = record(GenericData.Fixed(fixedSchema, byteArrayOf(0, 0, 0, 0, 0, 1, 4, 9))),
                schema = schema
            )
        }
    }
}

class ByteArrayEncoderTest : FunSpec({
    includeForEveryEncoder { byteArrayEncoderTests(it) }
})
