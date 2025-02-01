package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.serializer.AvroCollectionSerializer
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.internal.AbstractCollectionSerializer
import java.util.*

/**
 * This middleware is here to intercept some native types like kotlin Duration or ByteArray as we want to apply some
 * specific rules on them for generating custom schemas or having specific serialization strategies.
 */
@Suppress("UNCHECKED_CAST")
internal open class SerializationMiddleware(
    private val serializerOverrides: IdentityHashMap<KSerializer<*>, KSerializer<*>> = IdentityHashMap(),
    private val descriptorOverrides: IdentityHashMap<SerialDescriptor, SerialDescriptor> = IdentityHashMap(),
) {
    fun <T> apply(serializer: SerializationStrategy<T>): SerializationStrategy<T> {
        return serializerOverrides.getOrDefault(serializer as? KSerializer<*>, serializer) as SerializationStrategy<T>
    }

    @OptIn(InternalSerializationApi::class)
    fun <T> apply(deserializer: DeserializationStrategy<T>): DeserializationStrategy<T> {
        val output = serializerOverrides.getOrDefault(deserializer as? KSerializer<*>, deserializer) as DeserializationStrategy<T>
        // needs to wrap the collection deserializer to handle multiple array blocks
        if (output is AbstractCollectionSerializer<*, *, *> && output !is AvroCollectionSerializer<*>) {
            return AvroCollectionSerializer(output) as DeserializationStrategy<T>
        }
        return output
    }

    fun apply(descriptor: SerialDescriptor): SerialDescriptor {
        return descriptorOverrides.getOrDefault(descriptor, descriptor)
    }

    operator fun plus(other: SerializationMiddleware): SerializationMiddleware {
        return SerializationMiddleware(
            IdentityHashMap(serializerOverrides + other.serializerOverrides),
            IdentityHashMap(descriptorOverrides + other.descriptorOverrides)
        )
    }
}
