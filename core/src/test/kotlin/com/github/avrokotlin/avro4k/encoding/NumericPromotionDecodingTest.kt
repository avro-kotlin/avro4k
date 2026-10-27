package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.decodeFromGenericData
import com.github.avrokotlin.avro4k.encodeToBytesUsingApacheLib
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

// https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
// int is promotable to long, float, or double; long is promotable to float or double
internal class NumericPromotionDecodingTest : StringSpec({
    val intSchema = Schema.create(Schema.Type.INT)
    val longSchema = Schema.create(Schema.Type.LONG)

    "decode int as float" {
        Avro.decodeFromByteArray<Float>(intSchema, encodeToBytesUsingApacheLib(intSchema, 1)) shouldBe 1.0f
    }

    "decode int as double" {
        Avro.decodeFromByteArray<Double>(intSchema, encodeToBytesUsingApacheLib(intSchema, 1)) shouldBe 1.0
    }

    "decode long as float" {
        Avro.decodeFromByteArray<Float>(longSchema, encodeToBytesUsingApacheLib(longSchema, 2L)) shouldBe 2.0f
    }

    "decode long as double" {
        Avro.decodeFromByteArray<Double>(longSchema, encodeToBytesUsingApacheLib(longSchema, 2L)) shouldBe 2.0
    }

    "decode nullable int as nullable double" {
        val schema = Schema.createUnion(Schema.create(Schema.Type.NULL), intSchema)
        Avro.decodeFromByteArray<Double?>(schema, encodeToBytesUsingApacheLib(schema, 3)) shouldBe 3.0
        Avro.decodeFromByteArray<Double?>(schema, encodeToBytesUsingApacheLib(schema, null)) shouldBe null
    }

    "decode int and long record fields as float and double" {
        val schema =
            SchemaBuilder.record("FloatingPoints").fields()
                .name("f").type(intSchema).noDefault()
                .name("d").type(longSchema).noDefault()
                .endRecord()
        val record = GenericData.Record(schema).also {
            it.put("f", 4)
            it.put("d", 5L)
        }

        Avro.decodeFromByteArray<FloatingPoints>(schema, encodeToBytesUsingApacheLib(schema, record)) shouldBe FloatingPoints(4.0f, 5.0)
    }

    "decode int and long generic values as float and double" {
        @Suppress("DEPRECATION")
        Avro.decodeFromGenericData<Float>(intSchema, 6) shouldBe 6.0f
        @Suppress("DEPRECATION")
        Avro.decodeFromGenericData<Double>(intSchema, 6) shouldBe 6.0
        @Suppress("DEPRECATION")
        Avro.decodeFromGenericData<Float>(longSchema, 7L) shouldBe 7.0f
        @Suppress("DEPRECATION")
        Avro.decodeFromGenericData<Double>(longSchema, 7L) shouldBe 7.0
    }
}) {
    @Serializable
    private data class FloatingPoints(
        val f: Float,
        val d: Double,
    )
}