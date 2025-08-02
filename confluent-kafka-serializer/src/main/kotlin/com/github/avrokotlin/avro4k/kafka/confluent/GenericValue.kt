package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.schemaNotFoundInUnionError
import com.github.avrokotlin.avro4k.trySelectNamedSchema
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

/**
 * A [GenericContainer] that holds a value and a schema.
 *
 * In root level, it is used to hold a value and a schema to force a specific schema to the schema registry.
 * When used with a union's value with ambiguous types, it specifies explicitly the schema to use.
 * For the rest, it is ignored.
 */
@Serializable(with = GenericValueSerializer::class)
public data class GenericValue<T>(private val schema: Schema, public val value: T) : GenericContainer {
    override fun getSchema(): Schema = schema
}

private class GenericValueSerializer<T>(
    private val valueSerializer: KSerializer<T>,
) : KSerializer<GenericValue<T>> {
    override val descriptor: SerialDescriptor
        get() = valueSerializer.descriptor

    override fun serialize(encoder: Encoder, value: GenericValue<T>) {
        encoder as AvroEncoder
        // Resolve the union schema only if the GenericValue's schema is not a union, as we could provide a union for another purpose
        if (encoder.currentWriterSchema.isUnion && !value.schema.isUnion && !encoder.trySelectNamedSchema(value.schema)) {
            throw encoder.schemaNotFoundInUnionError(value.schema)
        }
        encoder.encodeSerializableValue(valueSerializer, value.value)
    }

    override fun deserialize(decoder: Decoder): GenericValue<T> {
        decoder as AvroDecoder
        return GenericValue(decoder.currentWriterSchema, decoder.decodeSerializableValue(valueSerializer))
    }
}