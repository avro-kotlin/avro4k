package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema.Wine
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData

class EnumEncoderTest : FunSpec({
    includeForEveryEncoder { enumEncoderTests(it) }
})

fun enumEncoderTests(enDecoder: EnDecoder): TestFactory {
    @Serializable
    data class MyWine(val wine: Wine)

    @Serializable
    data class NullableWine(val wine: Wine?)
    return stringSpec {
        "support enums" {
            enDecoder.testEncodeDecode(
                MyWine(Wine.Malbec),
                record(
                    GenericData.EnumSymbol(
                        null,
                        Wine.Malbec
                    )
                )
            )
        }

        "support nullable enums" {
            val enumSchema = enDecoder.avro.schema(NullableWine.serializer()).getField("wine").schema().types[1]
            enDecoder.testEncodeDecode(
                NullableWine(Wine.Shiraz),
                record(
                    GenericData.EnumSymbol(enumSchema, Wine.Shiraz)
                )
            )
            enDecoder.testEncodeDecode(NullableWine(null), record(null))
        }
    }
}