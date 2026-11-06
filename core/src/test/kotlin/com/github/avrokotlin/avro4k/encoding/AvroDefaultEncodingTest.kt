package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.SomeEnum
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.math.BigDecimal

internal class AvroDefaultEncodingTest : StringSpec({
    "test default values correctly decoded" {
        AvroAssertions.assertThat(ContainerWithoutDefaultFields("abc"))
            .isEncodedAs(record("abc"))
            .isDecodedAs(
                ContainerWithDefaultFields(
                    "abc",
                    "hello",
                    "hello",
                    null,
                    SomeEnum.B,
                    SomeEnum.B,
                    null,
                    1,
                    1,
                    true,
                    'a',
                    'a',
                    null,
                    1.23,
                    FooElement("foo"),
                    emptyList(),
                    listOf(FooElement("bar")),
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    null,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    null
                )
            )
    }
    "negative number defaults are decoded as numbers" {
        AvroAssertions.assertThat(ContainerWithoutDefaultFields("abc"))
            .isEncodedAs(record("abc"))
            .isDecodedAs(ContainerWithNegativeDefaults("abc", -1, -1L, Long.MIN_VALUE, -1.5f, -1.5, -1500.0, -Double.MAX_VALUE, -1, -1, -1500L, 1))
    }
    "string defaults that look like numbers are decoded as written" {
        AvroAssertions.assertThat(ContainerWithoutDefaultFields("abc"))
            .isEncodedAs(record("abc"))
            .isDecodedAs(ContainerWithStringDefaults("abc", "1", "-1", "12abc"))
    }
    "the schema and the decoder read a default the same way" {
        AvroAssertions.assertThat(ContainerWithoutDefaultFields("abc"))
            .isEncodedAs(record("abc"))
            .isDecodedAs(ContainerWithEdgeDefaults("abc", "a\tb", listOf(1L, 7L), "[1,2]", " 1", 1, -0.0))
    }
}) {
    @Serializable
    @SerialName("container")
    private data class ContainerWithEdgeDefaults(
        val name: String,
        @AvroDefault("\"a\tb\"")
        val tab: String,
        @AvroDefault("[1, 007]")
        val leadingZeros: List<Long>,
        @AvroDefault("[1,2]")
        val arrayLikeString: String,
        @AvroDefault(" 1")
        val spacedString: String,
        @AvroDefault(" +1 ")
        val plusOne: Int,
        @AvroDefault("-0.0")
        val negativeZero: Double,
    )

    @Serializable
    @SerialName("container")
    private data class ContainerWithStringDefaults(
        val name: String,
        @AvroDefault("1")
        val digits: String,
        @AvroDefault("-1")
        val negativeNumber: String,
        @AvroDefault("12abc")
        val startsWithDigits: String,
    )

    @Serializable
    private data class FooElement(
        val content: String,
    )

    @Serializable
    @SerialName("container")
    private data class ContainerWithoutDefaultFields(
        val name: String,
    )

    @Serializable
    @SerialName("container")
    private data class ContainerWithNegativeDefaults(
        val name: String,
        @AvroDefault("-1")
        val intDefault: Int,
        @AvroDefault("-1")
        val longDefault: Long,
        @AvroDefault("-9223372036854775808")
        val longMinDefault: Long,
        @AvroDefault("-1.5")
        val floatDefault: Float,
        @AvroDefault("-1.5")
        val doubleDefault: Double,
        @AvroDefault("-1.5e3")
        val doubleExponentDefault: Double,
        @AvroDefault("-1.7976931348623157E308")
        val doubleMinDefault: Double,
        @AvroDefault("-1")
        val intDefaultNullable: Int?,
        @AvroDefault("-1.0")
        val intFromDecimal: Int,
        @AvroDefault("-1.5e3")
        val longFromExponent: Long,
        @AvroDefault("1.0")
        val positiveIntFromDecimal: Int,
    )

    @Serializable
    @SerialName("container")
    private data class ContainerWithDefaultFields(
        val name: String,
        @AvroDefault("hello")
        val strDefault: String,
        @AvroDefault("hello")
        val strDefaultNullable: String?,
        @AvroDefault("null")
        val strDefaultNullableNull: String?,
        @AvroDefault("B")
        val enumDefault: SomeEnum,
        @AvroDefault("B")
        val enumDefaultNullable: SomeEnum?,
        @AvroDefault("null")
        val enumDefaultNullableNull: SomeEnum?,
        @AvroDefault("1")
        val intDefault: Int,
        @AvroDefault("1")
        val shouldBe1AndNot42: Int = 42,
        @AvroDefault("true")
        val booleanDefault: Boolean,
        @AvroDefault("a")
        val charDefault: Char,
        @AvroDefault("a")
        val charDefaultNullable: Char?,
        @AvroDefault("null")
        val charDefaultNullableNull: Char?,
        @AvroDefault("1.23")
        val doubleDefault: Double,
        @AvroDefault("""{"content":"foo"}""")
        val foo: FooElement,
        @AvroDefault("[]")
        val emptyFooList: List<FooElement>,
        @AvroDefault("""[{"content":"bar"}]""")
        val filledFooList: List<FooElement>,
        @Contextual
        @AvroDecimal(scale = 0, precision = 8)
        @AvroDefault("\u0000")
        val bigDecimal: BigDecimal,
        @Contextual
        @AvroDecimal(scale = 0, precision = 8)
        @AvroDefault("\u0000")
        val bigDecimalNullable: BigDecimal?,
        @Contextual
        @AvroDecimal(scale = 2, precision = 8)
        @AvroDefault("null")
        val bigDecimalNullableNull: BigDecimal?,
        @Contextual
        @AvroFixed(size = 16)
        @AvroDecimal(scale = 0, precision = 8)
        @AvroDefault("\u0000")
        val bigDecimalFixed: BigDecimal,
        @Contextual
        @AvroFixed(size = 16)
        @AvroDecimal(scale = 0, precision = 8)
        @AvroDefault("\u0000")
        val bigDecimalFixedNullable: BigDecimal?,
        @Contextual
        @AvroFixed(size = 16)
        @AvroDecimal(scale = 2, precision = 8)
        @AvroDefault("null")
        val bigDecimalFixedNullableNull: BigDecimal?,
        val kotlinDefault: Int = 42,
    )
}