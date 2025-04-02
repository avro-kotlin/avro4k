package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.schema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

class RecordDecodingCompatibilityTests : FunSpec() {
    init {
        test("should skip written boolean when absent from reading data class") {
            testCompatibility(
                Schema.create(Schema.Type.BOOLEAN),
                true
            )
        }
        test("should skip written int when absent from reading data class") {
            testCompatibility(
                Schema.create(Schema.Type.INT),
                42
            )
        }
        test("should skip written long when absent from reading data class") {
            testCompatibility(
                Schema.create(Schema.Type.LONG),
                17L
            )
        }
        test("should skip written float when absent from reading data class") {
            testCompatibility(
                Schema.create(Schema.Type.FLOAT),
                3.14f
            )
        }
        test("should skip written double when absent from reading data class") {
            testCompatibility(
                Schema.create(Schema.Type.DOUBLE),
                21.9
            )
        }
        test("should skip written string when absent from reading data class") {
            testCompatibility(
                Schema.create(Schema.Type.STRING),
                "text"
            )
        }
        test("should skip written bytes when absent from reading data class") {
            testCompatibility(
                Schema.create(Schema.Type.BYTES),
                byteArrayOf(1, 2, 3)
            )
        }
        test("should skip written fixed when absent from reading data class") {
            testCompatibility(
                Schema.createFixed("FixedName", null, null, 3),
                byteArrayOf(1, 2, 3)
            )
        }
        test("should skip written enum when absent from reading data class") {
            testCompatibility(
                Schema.createEnum("EnumName", null, null, listOf("A", "B")),
                "B"
            )
        }
        test("should skip written array when absent from reading data class") {
            testCompatibility(
                Schema.createArray(Schema.create(Schema.Type.INT)),
                listOf(4, 2)
            )
        }
        test("should skip written map when absent from reading data class") {
            testCompatibility(
                Schema.createMap(Schema.create(Schema.Type.STRING)),
                mapOf("key" to "value")
            )
        }
        test("should skip written null when absent from reading data class") {
            testCompatibility<Int?>(
                SchemaBuilder.nullable().intType(),
                null
            )
        }
        test("should skip written record when absent from reading data class") {
            testCompatibility<ReadDataClass>(
                Avro.schema<ReadDataClass>(),
                ReadDataClass("test", 42)
            )
        }
    }

    private inline fun <reified T> testCompatibility(
        skippedSchema: Schema,
        writtenData: T,
    ) {
        val schema =
            SchemaBuilder.record("WrittenDataClass")
                .fields()
                .name("firstField").type().stringType().noDefault()
                .name("skippedField").type(skippedSchema).noDefault()
                .name("secondField").type().intType().noDefault()
                .endRecord()
        val encodedBytes = Avro.encodeToByteArray(schema, WrittenDataClass("test", writtenData, 42))
        val decodedData = Avro.decodeFromByteArray<ReadDataClass>(schema, encodedBytes)

        decodedData shouldBe ReadDataClass("test", 42)
    }

    @Serializable
    @SerialName("WrittenDataClass")
    private data class WrittenDataClass<T>(val firstField: String, val skippedField: T, val secondField: Int)

    @Serializable
    @AvroAlias("WrittenDataClass")
    private data class ReadDataClass(val firstField: String, val secondField: Int)
}