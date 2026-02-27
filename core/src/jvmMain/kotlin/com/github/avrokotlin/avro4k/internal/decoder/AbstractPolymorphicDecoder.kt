package com.github.avrokotlin.avro4k.internal.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import com.github.avrokotlin.avro4k.internal.isNamedSchema
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@OptIn(ExperimentalSerializationApi::class)
internal abstract class AbstractPolymorphicDecoder(
    protected val avro: Avro,
    private val descriptor: SerialDescriptor,
    private val schema: Schema,
) : AbstractDecoder() {
    final override val serializersModule: SerializersModule
        get() = avro.serializersModule

    private lateinit var chosenSchema: Schema

    final override fun decodeString(): String {
        return tryFindSerialName()?.also { chosenSchema = it.second }?.first
            ?: throw SerializationException("Unknown schema name '${schema.fullName}' for polymorphic type ${descriptor.serialName}. Full schema: $schema")
    }

    private fun tryFindSerialName(): Pair<String, Schema>? {
        val namesAndAliasesToSerialName: Map<String, String> = avro.polymorphicResolver.getFullNamesAndAliasesToSerialName(descriptor)
        return tryFindSerialName(namesAndAliasesToSerialName, schema)
    }

    protected abstract fun tryFindSerialNameForUnion(
        namesAndAliasesToSerialName: Map<String, String>,
        schema: Schema,
    ): Pair<String, Schema>?

    protected fun tryFindSerialName(
        namesAndAliasesToSerialName: Map<String, String>,
        schema: Schema,
    ): Pair<String, Schema>? {
        if (schema.isUnion) {
            return tryFindSerialNameForUnion(namesAndAliasesToSerialName, schema)
        }
        return (
            namesAndAliasesToSerialName[schema.fullName]
                ?: schema.takeIf { it.isNamedSchema() }?.aliases?.firstNotNullOfOrNull { namesAndAliasesToSerialName[it] }
        )
            ?.let { it to schema }
    }

    final override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return newDecoder(chosenSchema)
            .decodeSerializableValue(deserializer)
    }

    abstract fun newDecoder(chosenSchema: Schema): Decoder

    final override fun decodeSequentially() = true

    final override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }
}