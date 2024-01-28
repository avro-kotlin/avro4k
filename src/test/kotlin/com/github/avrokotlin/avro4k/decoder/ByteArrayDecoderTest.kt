package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

class ByteArrayDecoderTest : StringSpec({
    val byteArray = byteArrayOf(1, 4, 9)
    listOf(
        "ByteBuffer" to ByteBuffer.wrap(byteArray),
        "ByteArray" to byteArray,
        "Array<Bytes>" to arrayOf<Byte>(1, 4, 9),
        "GenericData.Array" to
            GenericData.Array(
                Schema.createArray(Schema.create(Schema.Type.BYTES)),
                byteArray.toList()
            )
    ).forEach {
        "decode ${it.first} to ByteArray" {
            val schema = Avro.default.schema(ByteArrayTest.serializer())
            val record = GenericData.Record(schema)
            record.put("z", it.second)
            Avro.default.fromRecord(ByteArrayTest.serializer(), record).z shouldBe byteArrayOf(1, 4, 9)
        }
        "decode ${it.first} to List<Byte>" {
            val schema = Avro.default.schema(ListByteTest.serializer())
            val record = GenericData.Record(schema)
            record.put("z", it.second)
            Avro.default.fromRecord(ListByteTest.serializer(), record).z shouldBe listOf<Byte>(1, 4, 9)
        }
        "decode ${it.first} to Array<Byte>" {
            val schema = Avro.default.schema(ArrayByteTest.serializer())
            val record = GenericData.Record(schema)
            record.put("z", it.second)
            Avro.default.fromRecord(ArrayByteTest.serializer(), record).z shouldBe arrayOf<Byte>(1, 4, 9)
        }
    }
}) {
    @Serializable
    data class ByteArrayTest(val z: ByteArray)

    @Serializable
    data class ArrayByteTest(val z: Array<Byte>)

    @Serializable
    data class ListByteTest(val z: List<Byte>)
}