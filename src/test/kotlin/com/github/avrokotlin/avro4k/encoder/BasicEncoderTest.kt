package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.encode
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class BasicEncoderTest : WordSpec({

    "encode fields" should {
        "encode strings as UTF8" {

            val schema = Avro.default.schema(StringFoo.serializer())
            val record = Avro.default.encode(StringFoo.serializer(), StringFoo("hello"))
            record shouldBeContentOf ListRecord(schema, Utf8("hello"))
        }
        "encode strings as GenericFixed and pad bytes when schema is Type.FIXED" {

            val schema = SchemaBuilder.record("Foo").fields()
                    .name("s").type(Schema.createFixed("FixedString", null, null, 7)).noDefault()
                    .endRecord()

            val record = Avro.default.encode<StringFoo>(schema, StringFoo("hello"))
            record shouldBeContentOf
                    ListRecord(schema, GenericData.get().createFixed(null, byteArrayOf(104, 101, 108, 108, 111, 0, 0), schema.fields[0].schema()))
        }
        "encode longs" {

            val schema = Avro.default.schema(LongFoo.serializer())
            val record = Avro.default.encode(LongFoo.serializer(), LongFoo(123456L))
            record shouldBeContentOf ListRecord(schema, 123456L)
        }
        "encode doubles" {

            val schema = Avro.default.schema(DoubleFoo.serializer())
            val record = Avro.default.encode(DoubleFoo.serializer(), DoubleFoo(123.435))
            record shouldBeContentOf ListRecord(schema, 123.435)
        }
        "encode booleans" {

            val schema = Avro.default.schema(BooleanFoo.serializer())
            val record = Avro.default.encode(BooleanFoo.serializer(), BooleanFoo(true))
            record shouldBeContentOf ListRecord(schema, true)
        }
        "encode floats" {

            val schema = Avro.default.schema(FloatFoo.serializer())
            val record = Avro.default.encode(FloatFoo.serializer(), FloatFoo(123.435F))
            record shouldBeContentOf ListRecord(schema, 123.435F)
        }
        "encode ints" {

            val schema = Avro.default.schema(IntFoo.serializer())
            val record = Avro.default.encode(IntFoo.serializer(), IntFoo(123))
            record shouldBeContentOf ListRecord(schema, 123)
        }
        "encode shorts" {

            val schema = Avro.default.schema(ShortFoo.serializer())
            val record = Avro.default.encode(ShortFoo.serializer(), ShortFoo(123.toShort()))
            record shouldBeContentOf ListRecord(schema, 123)
        }
        "encode bytes" {

            val schema = Avro.default.schema(ByteFoo.serializer())
            val record = Avro.default.encode(ByteFoo.serializer(), ByteFoo(123.toByte()))
            record shouldBeContentOf ListRecord(schema, 123)
        }
    }
    "encode values" should {
        "encode strings as UTF8" {

            val schema = Schema.create(Schema.Type.STRING)
            val record = Avro.default.encode(schema, "hello")
            record shouldBe Utf8("hello")
        }
        "encode strings as GenericFixed and pad bytes when schema is Type.FIXED" {
            val schema = Schema.createFixed("FixedString", null, null, 7)

            val record = Avro.default.encode(schema, "hello")
            record shouldBe
                    GenericData.get().createFixed(null, byteArrayOf(104, 101, 108, 108, 111, 0, 0), schema)
        }
        "encode longs" {
            val schema = Schema.create(Schema.Type.LONG)
            val value = 123456L
            Avro.default.encode(schema, value) shouldBe value
        }
        "encode doubles" {
            val schema = Schema.create(Schema.Type.DOUBLE)
            val value = 123.435
            Avro.default.encode(schema, value) shouldBe value
        }
        "encode booleans" {
            val schema = Schema.create(Schema.Type.BOOLEAN)
            val value = true
            Avro.default.encode(schema, value) shouldBe value
        }
        "encode floats" {
            val schema = Schema.create(Schema.Type.FLOAT)
            val value = 123.456f
            Avro.default.encode(schema, value) shouldBe value
        }
        "encode ints" {
            val schema = Schema.create(Schema.Type.INT)
            val value = 123
            Avro.default.encode(schema, value) shouldBe value
        }
        "encode shorts" {
            val schema = Schema.create(Schema.Type.INT)
            val value = 123.toShort()
            Avro.default.encode(schema, value) shouldBe value.toInt()
        }
        "encode bytes" {
            val schema = Schema.create(Schema.Type.INT)
            val value = 123.toByte()
            Avro.default.encode(schema, value) shouldBe value.toInt()
        }
    }

}) {
    @Serializable
    data class StringFoo(val s: String)

    @Serializable
    data class LongFoo(val l: Long)

    @Serializable
    data class DoubleFoo(val d: Double)

    @Serializable
    data class BooleanFoo(val d: Boolean)

    @Serializable
    data class FloatFoo(val d: Float)

    @Serializable
    data class IntFoo(val i: Int)

    @Serializable
    data class ShortFoo(val s: Short)

    @Serializable
    data class ByteFoo(val b: Byte)
}
