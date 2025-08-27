package com.github.avrokotlin.avro4k.internal.encoder.generic

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@OptIn(ExperimentalSerializationApi::class)
internal class PolymorphicEncoder(
    private val avro: Avro,
    private val schema: Schema,
    private val onEncoded: (Any) -> Unit,
) : AbstractEncoder() {
    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        // index 0 is the type discriminator, index 1 is the value itself
        // we don't need the type discriminator here
        return index == 1
    }

    override fun <T> encodeSerializableValue(
        serializer: SerializationStrategy<T>,
        value: T,
    ) {
        // Here we don't need to resolve the union, as it is already resolved inside AvroTaggedEncoder.beginStructure
        AvroValueGenericEncoder(avro, schema) {
            onEncoded(it ?: throw UnsupportedOperationException("Polymorphic types cannot encode null values"))
        }.encodeSerializableValue(serializer, value)
    }
}