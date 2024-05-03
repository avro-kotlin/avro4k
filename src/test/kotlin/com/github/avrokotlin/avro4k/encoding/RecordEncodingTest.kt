package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.record
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import org.apache.avro.SchemaBuilder

class RecordEncodingTest : StringSpec({
    "encoding basic data class" {
        val input =
            Foo(
                "string value",
                2.2,
                true,
                S(setOf(1, 2, 3)),
                ValueClass(ByteArray(3) { it.toByte() })
            )

        AvroAssertions.assertThat<Foo>()
            .generatesSchema(
                SchemaBuilder.record("Foo").fields()
                    .name("a").type().stringType().noDefault()
                    .name("b").type().doubleType().noDefault()
                    .name("c").type().booleanType().noDefault()
                    .name("s").type(
                        SchemaBuilder.record("S").fields()
                            .name("t").type().array().items().intType().noDefault()
                            .endRecord()
                    ).noDefault()
                    .name("vc").type().bytesType().noDefault()
                    .endRecord()
            )
        AvroAssertions.assertThat(input)
            .isEncodedAs(
                record(
                    "string value",
                    2.2,
                    true,
                    record(setOf(1, 2, 3)),
                    ByteArray(3) { it.toByte() }
                )
            )
    }
    "encode/decode strings as UTF8" {
        AvroAssertions.assertThat(StringFoo("hello"))
            .isEncodedAs(record("hello"))
    }
    "encode/decode strings as GenericFixed and pad bytes when schema is Type.FIXED" {
        @Serializable
        @SerialName("Foo")
        data class StringFoo(
            @AvroFixed(7) val a: String?,
        )

        AvroAssertions.assertThat(StringFoo("hello"))
            .isEncodedAs(
                record(byteArrayOf(104, 101, 108, 108, 111, 0, 0)),
                StringFoo(String("hello".toByteArray() + byteArrayOf(0, 0)))
            )
    }
    "encode/decode nullable string" {
        @Serializable
        data class NullableString(val a: String?)

        AvroAssertions.assertThat(NullableString("hello"))
            .isEncodedAs(record("hello"))
        AvroAssertions.assertThat(NullableString(null))
            .isEncodedAs(record(null))
    }
    "encode/decode longs" {
        @Serializable
        data class LongFoo(val l: Long)
        AvroAssertions.assertThat(LongFoo(123456L))
            .isEncodedAs(record(123456L))
    }
    "encode/decode doubles" {
        @Serializable
        data class DoubleFoo(val d: Double)
        AvroAssertions.assertThat(DoubleFoo(123.435))
            .isEncodedAs(record(123.435))
    }
    "encode/decode booleans" {
        @Serializable
        data class BooleanFoo(val d: Boolean)
        AvroAssertions.assertThat(BooleanFoo(false))
            .isEncodedAs(record(false))
    }
    "encode/decode nullable booleans" {
        @Serializable
        data class NullableBoolean(val a: Boolean?)

        AvroAssertions.assertThat(NullableBoolean(true))
            .isEncodedAs(record(true))
        AvroAssertions.assertThat(NullableBoolean(null))
            .isEncodedAs(record(null))
    }
    "encode/decode floats" {
        @Serializable
        data class FloatFoo(val d: Float)
        AvroAssertions.assertThat(FloatFoo(123.435F))
            .isEncodedAs(record(123.435F))
    }
    "encode/decode ints" {
        @Serializable
        data class IntFoo(val i: Int)
        AvroAssertions.assertThat(IntFoo(123))
            .isEncodedAs(record(123))
    }
    "encode/decode shorts" {
        @Serializable
        data class ShortFoo(val s: Short)
        AvroAssertions.assertThat(ShortFoo(123))
            .isEncodedAs(record(123))
    }
    "encode/decode bytes" {
        @Serializable
        data class ByteFoo(val b: Byte)
        AvroAssertions.assertThat(ByteFoo(123))
            .isEncodedAs(record(123))
    }
    "should not encode records with a different name" {
        @Serializable
        data class TheRecord(val v: Int)
        shouldThrow<SerializationException> {
            val wrongRecordSchema = SchemaBuilder.record("AnotherRecord").fields().name("v").type().intType().noDefault().endRecord()
            AvroAssertions.assertThat(TheRecord(1))
                .isEncodedAs(record(1), writerSchema = wrongRecordSchema)
        }
    }
    "support objects" {
        AvroAssertions.assertThat(ObjectClass)
            .isEncodedAs(record())
    }
}) {
    @Serializable
    object ObjectClass {
        val field1 = "ignored"
    }

    @Serializable
    private data class StringFoo(val s: String)

    @Serializable
    @SerialName("Foo")
    private data class Foo(val a: String, val b: Double, val c: Boolean, val s: S, val vc: ValueClass) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Foo) return false

            if (a != other.a) return false
            if (b != other.b) return false
            if (c != other.c) return false
            if (s != other.s) return false
            if (!vc.value.contentEquals(other.vc.value)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = a.hashCode()
            result = 31 * result + b.hashCode()
            result = 31 * result + c.hashCode()
            result = 31 * result + s.hashCode()
            result = 31 * result + vc.value.contentHashCode()
            return result
        }
    }

    @Serializable
    @SerialName("S")
    private data class S(val t: Set<Int>)

    @JvmInline
    @Serializable
    private value class ValueClass(val value: ByteArray)
}