package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.AvroDecoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

/**
 * A generic container for a map with a schema. Native avro normally accept only string keys,
 * while avro4k allows almost any type of key, the reason why this class allows any type of key.
 *
 * This class is needed only when serializing a map at the root level to specify the schema of the map,
 * as we cannot infer the schema from the values.
 *
 * @property schema the schema of the map (must be of type MAP)
 */
@Serializable(with = GenericMapSerializer::class)
public class GenericMap<K, V>(private val schema: Schema, private val map: Map<K, V>) : GenericContainer, Map<K, V> by map {
    init {
        require(schema.type == Schema.Type.MAP) { "Schema must be of type MAP" }
    }

    override fun getSchema(): Schema = schema
}

private class GenericMapSerializer<K, V>(
    keySerializer: KSerializer<K>,
    valueSerializer: KSerializer<V>,
) : KSerializer<GenericMap<K, V>> {
    private val mapSerializer = MapSerializer(keySerializer, valueSerializer)
    override val descriptor: SerialDescriptor get() = mapSerializer.descriptor

    override fun serialize(encoder: Encoder, value: GenericMap<K, V>) {
        // No need to resolve the union schema here, as we can only have one MAP in a union, so delegating the type checks to the basic map encoding
        encoder.encodeSerializableValue(mapSerializer, value)
    }

    override fun deserialize(decoder: Decoder): GenericMap<K, V> {
        decoder as AvroDecoder
        return GenericMap(decoder.currentWriterSchema, decoder.decodeSerializableValue(mapSerializer))
    }
}