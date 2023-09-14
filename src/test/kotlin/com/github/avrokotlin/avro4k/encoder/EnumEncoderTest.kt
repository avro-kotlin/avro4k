package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.decoder.MyWine
import com.github.avrokotlin.avro4k.decoder.NullableWine
import com.github.avrokotlin.avro4k.schema.Wine
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import org.apache.avro.generic.GenericData

fun enumEncoderTests(encoderToTest: EncoderToTest): TestFactory {
    return funSpec {
        test("support enums") {
            encoderToTest.testEncodeDecode(
                MyWine(Wine.Malbec), record(
                    GenericData.EnumSymbol(
                        null, Wine.Malbec
                    )
                )
            )
        }

        test("support nullable enums") {
            val enumSchema = encoderToTest.avro.schema(NullableWine.serializer()).getField("wine").schema().types[1]
            encoderToTest.testEncodeDecode(NullableWine(Wine.Shiraz), record(
                GenericData.EnumSymbol(enumSchema, Wine.Shiraz)
            ))
            encoderToTest.testEncodeDecode(NullableWine(null), record(null))
        }
    }
}

class EnumEncoderTest : FunSpec({
    includeForEveryEncoder { enumEncoderTests(it) }
})