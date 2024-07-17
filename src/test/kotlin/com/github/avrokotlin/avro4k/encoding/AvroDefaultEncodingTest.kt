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
}) {
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