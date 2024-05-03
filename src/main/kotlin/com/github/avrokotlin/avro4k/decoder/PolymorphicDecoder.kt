package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import com.github.avrokotlin.avro4k.schema.findAnnotation
import com.github.avrokotlin.avro4k.schema.nonNullSerialName
import com.github.avrokotlin.avro4k.schema.possibleSerializationSubclasses
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

internal class PolymorphicDecoder(
    private val avro: Avro,
    private val descriptor: SerialDescriptor,
    private val value: IndexedRecord,
) : AbstractDecoder() {
    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    private val namesAndAliasesToSerialName: Map<String, String> =
        descriptor.possibleSerializationSubclasses(serializersModule)
            .flatMap {
                sequence {
                    yield(it.nonNullSerialName to it.nonNullSerialName)
                    it.findAnnotation<AvroAlias>()?.value?.forEach { alias ->
                        yield(alias to it.nonNullSerialName)
                    }
                }
            }.toMap()

    override fun decodeString(): String {
        return tryFindSerialName(value.schema)
            ?: throw SerializationException("Unknown schema name ${value.schema.fullName} for polymorphic type ${descriptor.serialName}")
    }

    private fun tryFindSerialName(schema: Schema): String? {
        return namesAndAliasesToSerialName[schema.fullName]
            ?: schema.aliases.firstNotNullOfOrNull { namesAndAliasesToSerialName[it] }
    }

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return AvroValueDecoder(avro, value, value.schema)
            .decodeSerializableValue(deserializer)
    }

    override fun decodeSequentially() = true

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }
}