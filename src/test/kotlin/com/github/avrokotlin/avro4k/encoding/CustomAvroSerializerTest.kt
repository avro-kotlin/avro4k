package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.mockk
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

class CustomAvroSerializerTest : StringSpec({
    "Non Avro encoder or decoder should fail" {
        class CustomSerializer : BasicSerializer()
        shouldThrow<UnsupportedOperationException> {
            CustomSerializer().serialize(CustomEncoder(), object {})
        }
        shouldThrow<UnsupportedOperationException> {
            CustomSerializer().deserialize(CustomDecoder())
        }
    }
    "Non Avro encoder or decoder should not fail when generic methods are implemented" {
        var serializeGenericCalled = false
        val expectedDeserializedValue = object {}

        class CustomSerializer : BasicSerializer() {
            override fun deserializeGeneric(decoder: Decoder): Any {
                return expectedDeserializedValue
            }

            override fun serializeGeneric(encoder: Encoder, value: Any) {
                serializeGenericCalled = true
            }
        }
        CustomSerializer().serialize(CustomEncoder(), object {})
        serializeGenericCalled shouldBe true

        CustomSerializer().deserialize(CustomDecoder()) shouldBe expectedDeserializedValue
    }
    "Should not fail when using Avro encoder or decoder" {
        var serializeAvroCalled = false
        val expectedDeserializedValue = object {}

        class CustomSerializer : BasicSerializer() {
            override fun serializeAvro(encoder: AvroEncoder, value: Any) {
                value shouldBe expectedDeserializedValue
                serializeAvroCalled = true
            }

            override fun deserializeAvro(decoder: AvroDecoder): Any {
                return expectedDeserializedValue
            }
        }
        CustomSerializer().serialize(mockk<AvroEncoder>(), expectedDeserializedValue)
        serializeAvroCalled shouldBe true

        CustomSerializer().deserialize(mockk<AvroDecoder>()) shouldBe expectedDeserializedValue
    }
    "Supports null to true should make the descriptor nullable" {
        class CustomSerializer : BasicSerializer() {
            override val supportsNull: Boolean
                get() = true
        }
        CustomSerializer().descriptor.isNullable shouldBe true
    }
    "descriptor should not be nullable by default" {
        class CustomSerializer : BasicSerializer()
        CustomSerializer().descriptor.isNullable shouldBe false
    }
})

private abstract class BasicSerializer : AvroSerializer<Any>("basic") {
    override fun serializeAvro(encoder: AvroEncoder, value: Any) {
        TODO("Not yet implemented")
    }

    override fun deserializeAvro(decoder: AvroDecoder): Any {
        TODO("Not yet implemented")
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        TODO("Not yet implemented")
    }
}

private class CustomEncoder : AbstractEncoder() {
    override val serializersModule: SerializersModule
        get() = EmptySerializersModule()
}

private class CustomDecoder : AbstractDecoder() {
    override val serializersModule: SerializersModule
        get() = EmptySerializersModule()

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        TODO("Not yet implemented")
    }
}