package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDecimalLogicalType
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.io.AvroDecodeFormat
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToByteArray
import org.apache.avro.generic.GenericData
import java.math.BigDecimal

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
    @AvroDefault("true")
    val booleanDefault: Boolean,
    @AvroDefault("1.23")
    val doubleDefault: Double,
    @AvroDefault(Avro.NULL)
    val nullableStr: String?,
    @AvroDefault("""{"content":"foo"}""")
    val foo: FooElement,
    @AvroDefault("[]")
    val emptyFooList: List<FooElement>,
    @AvroDefault("""[{"content":"bar"}]""")
    val filledFooList: List<FooElement>,
    @AvroDefault("\u0000")
    @AvroDecimalLogicalType(0, 10)
    @Serializable(BigDecimalSerializer::class)
    val bigDecimal: BigDecimal,
)

@Serializable
@AvroEnumDefault("UNKNOWN")
enum class EnumWithDefault {
    UNKNOWN,
    A,
}

@Serializable
@AvroEnumDefault("UNKNOWN")
enum class FutureEnumWithDefault {
    UNKNOWN,
    A,
    C,
}

@Serializable
data class Wrap(val value: EnumWithDefault)

@Serializable
data class FutureWrap(val value: FutureEnumWithDefault)

class AvroDefaultValuesDecoderTest : FunSpec({
    test("test default values correctly decoded") {
        val name = "abc"
        val writerSchema = Avro.default.schema(ContainerWithoutDefaultFields.serializer())
        val record = GenericData.Record(writerSchema)
        record.put("name", name)

        val byteArray = Avro.default.encodeToByteArray(ContainerWithoutDefaultFields("abc"))
        val deserialized =
            Avro.default.openInputStream(ContainerWithDefaultFields.serializer()) {
                decodeFormat = AvroDecodeFormat.Data(writerSchema, defaultReadSchema)
            }.from(byteArray).nextOrThrow()
        deserialized.name.shouldBe("abc")
        deserialized.strDefault.shouldBe("hello")
        deserialized.intDefault.shouldBe(1)
        deserialized.booleanDefault.shouldBe(true)
        deserialized.doubleDefault.shouldBe(1.23)
        deserialized.nullableStr.shouldBeNull()
        deserialized.foo.content.shouldBe("foo")
        deserialized.emptyFooList.shouldBeEmpty()
        deserialized.filledFooList.shouldContainExactly(FooElement("bar"))
        deserialized.bigDecimal.shouldBe(BigDecimal.ZERO)
    }
    test("Decoding enum with an unknown or future value uses default value") {
        val encoded =
            Avro.default.encodeToByteArray(
                FutureWrap.serializer(),
                FutureWrap(FutureEnumWithDefault.C)
            )
        val decoded = Avro.default.decodeFromByteArray(Wrap.serializer(), encoded)

        decoded shouldBe Wrap(EnumWithDefault.UNKNOWN)
    }
})