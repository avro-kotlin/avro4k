package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.AvroStringable
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.math.BigDecimal
import kotlin.time.Duration.Companion.days

internal class AvroStringableEncodingTest : StringSpec({
    listOf(
        true to "true",
        1.toByte() to "1",
        2.toShort() to "2",
        3 to "3",
        4L to "4",
        5.0f to "5.0",
        6.0 to "6.0",
        '7' to "7",
        "1234567" to "1234567",
        1.5.days to "P1DT43200S",
        BigDecimal("1234567890.1234567890") to "1234567890.1234567890",
        java.time.Duration.parse("PT36H") to "P1DT43200S",
        java.time.Period.parse("P3Y4D") to "P36M4D",
        java.time.LocalTime.parse("12:34:56") to "12:34:56",
        java.time.LocalDate.parse("2021-01-01") to "2021-01-01",
        java.time.LocalDateTime.parse("2021-01-01T12:34:56") to "2021-01-01T12:34:56",
        java.time.Instant.parse("2020-01-01T12:34:56Z") to "2020-01-01T12:34:56Z"
    ).forEach { (value, stringifiedValue) ->
        "${value::class.qualifiedName}: data class property" {
            AvroAssertions.assertThat(StringifiedDataClass(value), StringifiedDataClass.serializer(Avro.serializersModule.serializer(value::class, emptyList(), false)))
                .generatesSchema(
                    SchemaBuilder.record(StringifiedDataClass::class.qualifiedName).fields()
                        .name("value").type(Schema.create(Schema.Type.STRING)).noDefault()
                        .endRecord()
                )
                .isEncodedAs(record(stringifiedValue))
        }
        "${value::class.qualifiedName}: nullable data class property" {
            AvroAssertions.assertThat(StringifiedDataClass(value), StringifiedDataClass.serializer(Avro.serializersModule.serializer(value::class, emptyList(), true)))
                .generatesSchema(
                    SchemaBuilder.record(StringifiedDataClass::class.qualifiedName).fields()
                        .name("value").type(Schema.create(Schema.Type.STRING).nullable).withDefault(null)
                        .endRecord()
                )
                .isEncodedAs(record(stringifiedValue))
            AvroAssertions.assertThat(StringifiedDataClass(null), StringifiedDataClass.serializer(Avro.serializersModule.serializer(value::class, emptyList(), true)))
                .generatesSchema(
                    SchemaBuilder.record(StringifiedDataClass::class.qualifiedName).fields()
                        .name("value").type(Schema.create(Schema.Type.STRING).nullable).withDefault(null)
                        .endRecord()
                )
                .isEncodedAs(record(null))
        }
        "${value::class.qualifiedName}: value class" {
            AvroAssertions.assertThat(StringifiedValueClass(value), StringifiedValueClass.serializer(Avro.serializersModule.serializer(value::class, emptyList(), false)))
                .generatesSchema(Schema.create(Schema.Type.STRING))
                .isEncodedAs(stringifiedValue, decodedComparator = { a, b -> a.value shouldBe b.value })
        }
        "${value::class.qualifiedName}: nullable value class" {
            AvroAssertions.assertThat(StringifiedValueClass(value), StringifiedValueClass.serializer(Avro.serializersModule.serializer(value::class, emptyList(), true)))
                .generatesSchema(Schema.create(Schema.Type.STRING).nullable)
                .isEncodedAs(stringifiedValue, decodedComparator = { a, b -> a.value shouldBe b.value })
            AvroAssertions.assertThat(StringifiedValueClass(null), StringifiedValueClass.serializer(Avro.serializersModule.serializer(value::class, emptyList(), true)))
                .generatesSchema(Schema.create(Schema.Type.STRING).nullable)
                .isEncodedAs(null)
        }
    }

    "ByteArray: data class property" {
        AvroAssertions.assertThat(StringifiedByteArrayDataClass("hello".toByteArray()))
            .generatesSchema(
                SchemaBuilder.record(StringifiedByteArrayDataClass::class.qualifiedName).fields()
                    .name("value").type(Schema.create(Schema.Type.STRING)).noDefault()
                    .endRecord()
            )
            .isEncodedAs(record("hello"))
    }
    "ByteArray: nullable data class property" {
        AvroAssertions.assertThat(StringifiedNullableByteArrayDataClass("hello".toByteArray()))
            .generatesSchema(
                SchemaBuilder.record(StringifiedNullableByteArrayDataClass::class.qualifiedName).fields()
                    .name("value").type(Schema.create(Schema.Type.STRING).nullable).withDefault(null)
                    .endRecord()
            )
            .isEncodedAs(record("hello"))
        AvroAssertions.assertThat(StringifiedNullableByteArrayDataClass(null))
            .generatesSchema(
                SchemaBuilder.record(StringifiedNullableByteArrayDataClass::class.qualifiedName).fields()
                    .name("value").type(Schema.create(Schema.Type.STRING).nullable).withDefault(null)
                    .endRecord()
            )
            .isEncodedAs(record(null))
    }
    "ByteArray: value class" {
        AvroAssertions.assertThat(StringifiedValueClass("hello".toByteArray()), StringifiedValueClass.serializer(ByteArraySerializer()))
            .generatesSchema(Schema.create(Schema.Type.STRING))
            .isEncodedAs("hello", decodedComparator = { a, b -> a.value shouldBe b.value })
    }
    "ByteArray: nullable value class" {
        AvroAssertions.assertThat(StringifiedValueClass("hello".toByteArray()), StringifiedValueClass.serializer(ByteArraySerializer().nullable))
            .generatesSchema(Schema.create(Schema.Type.STRING).nullable)
            .isEncodedAs("hello", decodedComparator = { a, b -> a.value shouldBe b.value })
        AvroAssertions.assertThat(StringifiedValueClass(null), StringifiedValueClass.serializer(ByteArraySerializer().nullable))
            .generatesSchema(Schema.create(Schema.Type.STRING).nullable)
            .isEncodedAs(null)
    }
}) {
    @Serializable
    private data class StringifiedByteArrayDataClass(
        @AvroFixed(10) // ignored as @AvroStringable takes precedence
        @AvroStringable
        val value: ByteArray,
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as StringifiedByteArrayDataClass

            return value.contentEquals(other.value)
        }

        override fun hashCode(): Int {
            return value.contentHashCode()
        }
    }

    @Serializable
    private data class StringifiedNullableByteArrayDataClass(
        @AvroFixed(10) // ignored as @AvroStringable takes precedence
        @AvroStringable
        val value: ByteArray?,
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as StringifiedNullableByteArrayDataClass

            if (value != null) {
                if (other.value == null) return false
                if (!value.contentEquals(other.value)) return false
            } else if (other.value != null) {
                return false
            }

            return true
        }

        override fun hashCode(): Int {
            return value?.contentHashCode() ?: 0
        }
    }

    @Serializable
    private data class StringifiedDataClass<T>(
        @AvroFixed(10) // ignored as @AvroStringable takes precedence
        @AvroStringable
        val value: T,
    )

    @JvmInline
    @Serializable
    private value class StringifiedValueClass<T>(
        @AvroFixed(10) // ignored as @AvroStringable takes precedence
        @AvroStringable
        val value: T,
    )
}