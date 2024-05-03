package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.math.BigDecimal

class AvroDefaultEncodingTest : StringSpec({
    "test default values correctly decoded" {
        AvroAssertions.assertThat(ContainerWithoutDefaultFields("abc"))
            .isEncodedAs(record("abc"))
            .isDecodedAs(
                ContainerWithDefaultFields(
                    "abc",
                    "hello",
                    1,
                    1,
                    true,
                    1.23,
                    null,
                    FooElement("foo"),
                    emptyList(),
                    listOf(FooElement("bar")),
                    BigDecimal.ZERO
                )
            )
    }
}) {
    @Serializable
    data class FooElement(
        val content: String,
    )

    @Serializable
    @SerialName("container")
    data class ContainerWithoutDefaultFields(
        val name: String,
    )

    @Serializable
    @SerialName("container")
    data class ContainerWithDefaultFields(
        val name: String,
        @AvroDefault("hello")
        val strDefault: String,
        @AvroDefault("1")
        val intDefault: Int,
        @AvroDefault("1")
        val shouldBe1AndNot42: Int = 42,
        @AvroDefault("true")
        val booleanDefault: Boolean,
        @AvroDefault("1.23")
        val doubleDefault: Double,
        @AvroDefault("null")
        val nullableStr: String?,
        @AvroDefault("""{"content":"foo"}""")
        val foo: FooElement,
        @AvroDefault("[]")
        val emptyFooList: List<FooElement>,
        @AvroDefault("""[{"content":"bar"}]""")
        val filledFooList: List<FooElement>,
        @AvroDefault("\u0000")
        @AvroDecimal(0, 10)
        @Serializable(BigDecimalSerializer::class)
        val bigDecimal: BigDecimal,
        val kotlinDefault: Int = 42,
    )
}