package com.github.avrokotlin.avro4k.encoder

import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.WordSpec
import io.kotest.core.spec.style.wordSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

fun recordEncodingTests(encoderToTest: EncoderToTest): TestFactory {
    @Serializable
    data class StringFoo(val s: String)
    return wordSpec {
        "Encoder" should {
            "encode strings as UTF8" {
                encoderToTest.testEncodeDecode(StringFoo("hello"), record("hello"))
            }
            "encode strings as GenericFixed and pad bytes when schema is Type.FIXED" {
                val fixedSchema = Schema.createFixed("FixedString", null, null, 7)
                val schema = SchemaBuilder.record("Foo").fields()
                    .name("s").type(fixedSchema).noDefault()
                    .endRecord()
                val encoded = encoderToTest.testEncodeIsEqual(
                    value = StringFoo("hello"),
                    shouldMatch = record(GenericData.Fixed(fixedSchema, byteArrayOf(104, 101, 108, 108, 111, 0, 0))),
                    schema = schema
                )
                encoderToTest.testDecodeIsEqual(encoded, StringFoo(String("hello".toByteArray() + byteArrayOf(0, 0))))
            }
            "encode nullable string" {
                @Serializable
                data class NullableString(val a: String?)
                encoderToTest.testEncodeDecode(NullableString("hello"), record("hello"))
                encoderToTest.testEncodeDecode(NullableString(null), record(null))
            }
            "encode longs" {
                @Serializable
                data class LongFoo(val l: Long)
                encoderToTest.testEncodeDecode(LongFoo(123456L), record(123456L))
            }
            "encode doubles" {

                @Serializable
                data class DoubleFoo(val d: Double)
                encoderToTest.testEncodeDecode(DoubleFoo(123.435), record(123.435))
            }
            "encode booleans" {
                @Serializable
                data class BooleanFoo(val d: Boolean)
                encoderToTest.testEncodeDecode(BooleanFoo(false), record(false))
            }
            "encode nullable booleans" {
                @Serializable
                data class NullableBoolean(val a: Boolean?)
                encoderToTest.testEncodeDecode(NullableBoolean(true), record(true))
                encoderToTest.testEncodeDecode(NullableBoolean(null), record(null))
            }
            "encode floats" {
                @Serializable
                data class FloatFoo(val d: Float)
                encoderToTest.testEncodeDecode(FloatFoo(123.435F), record(123.435F))
            }
            "encode ints" {
                @Serializable
                data class IntFoo(val i: Int)
                encoderToTest.testEncodeDecode(IntFoo(123), record(123))
            }
            "encode shorts" {
                @Serializable
                data class ShortFoo(val s: Short)
                encoderToTest.testEncodeDecode(ShortFoo(123.toShort()), record(123.toShort()))
            }
            "encode bytes" {
                @Serializable
                data class ByteFoo(val b: Byte)
                encoderToTest.testEncodeDecode(ByteFoo(123.toByte()), record(123.toByte()))
            }
        }
    }
}

class RecordEncodingTests : WordSpec({
    includeForEveryEncoder { recordEncodingTests(it) }
})