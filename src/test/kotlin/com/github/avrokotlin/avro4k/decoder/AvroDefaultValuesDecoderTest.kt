package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.*
import com.github.avrokotlin.avro4k.endecode.EnDecoder
import com.github.avrokotlin.avro4k.endecode.includeForEveryEncoder
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.spec.style.stringSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
import java.math.BigDecimal

@Serializable
data class FooElement(
    val content: String
)

@Serializable
@AvroName("container")
data class ContainerWithoutDefaultFields(
    val name: String
)

@Serializable
@AvroName("container")
data class ContainerWithDefaultFields(
    val name: String,
    @AvroDefault("hello")
    val strDefault: String,
    @AvroDefault("bytearray")
    val byteArrayDefault: ByteArray,
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
    @ScalePrecision(0, 10)
    @Serializable(BigDecimalSerializer::class)
    val bigDecimal: BigDecimal
)

@Serializable
@AvroEnumDefault("UNKNOWN")
enum class EnumWithDefault {
    A, UNKNOWN
}

@Serializable
@AvroEnumDefault("UNKNOWN")
@SerialName("EnumWithDefault")
enum class FutureEnumWithDefault {
    C, UNKNOWN, A
}

@Serializable
data class Wrap(val value: EnumWithDefault)

@Serializable
data class FutureWrap(val value: FutureEnumWithDefault)
class AvroDefaultValuesDecoderTest : StringSpec({
    includeForEveryEncoder { avroDefaultValuesDecodeTess(it) }
})
fun <T : Any> avroDefaultValuesDecodeTess(enDecoder: EnDecoder<T>): TestFactory {
    return stringSpec {
        "test default values correctly decoded" {
            val name = "abc"
            val writerSchema = Avro.default.schema(ContainerWithoutDefaultFields.serializer())
            val record = GenericData.Record(writerSchema)
            record.put("name", name)
            val serialized = enDecoder.encodeGenericRecordForComparison(record, writerSchema)
            val deserialized = enDecoder.decode(
                serialized = serialized , 
                deserializer = ContainerWithDefaultFields.serializer(), 
                writeSchema = writerSchema
                
            )
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
            deserialized.byteArrayDefault shouldBe "bytearray".toByteArray()

        }
        "Decoding enum with an unknown or future value uses default value" {
            val writeSerializer = FutureWrap.serializer()
            val writeSchema = enDecoder.avro.schema(writeSerializer)
            
            val encoded =enDecoder.encode(FutureWrap(FutureEnumWithDefault.C), writeSerializer, writeSchema)
            
            val decoded = enDecoder.decode(encoded, Wrap.serializer(), writeSchema = writeSchema)

            decoded shouldBe Wrap(EnumWithDefault.UNKNOWN)
        }
    }
}