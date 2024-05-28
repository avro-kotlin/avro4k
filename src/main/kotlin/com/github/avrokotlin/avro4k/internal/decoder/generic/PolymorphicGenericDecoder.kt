package com.github.avrokotlin.avro4k.internal.decoder.generic

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

internal class PolymorphicGenericDecoder(
    private val avro: Avro,
    private val descriptor: SerialDescriptor,
    private val value: IndexedRecord,
) : AbstractDecoder() {
    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun decodeString(): String {
        return tryFindSerialName(value.schema)
            ?: throw SerializationException("Unknown schema name ${value.schema.fullName} for polymorphic type ${descriptor.serialName}")
    }

    private fun tryFindSerialName(schema: Schema): String? {
        val namesAndAliasesToSerialName = avro.polymorphicResolver.getFullNamesAndAliasesToSerialName(descriptor)
        return namesAndAliasesToSerialName[schema.fullName]
            ?: schema.aliases.firstNotNullOfOrNull { namesAndAliasesToSerialName[it] }
    }

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return AvroValueGenericDecoder(avro, value, value.schema)
            .decodeSerializableValue(deserializer)
    }

    override fun decodeSequentially() = true

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }
}