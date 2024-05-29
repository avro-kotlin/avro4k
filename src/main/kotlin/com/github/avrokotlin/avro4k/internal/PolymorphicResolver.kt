package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.AvroAlias
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.util.WeakIdentityHashMap

internal class PolymorphicResolver(private val serializersModule: SerializersModule) {
    private val cache = WeakIdentityHashMap<SerialDescriptor, Map<String, String>>()

    fun getFullNamesAndAliasesToSerialName(descriptor: SerialDescriptor): Map<String, String> {
        return cache.getOrPut(descriptor) {
            descriptor.possibleSerializationSubclasses(serializersModule)
                .flatMap {
                    sequence {
                        yield(it.nonNullSerialName to it.nonNullSerialName)
                        it.findAnnotation<AvroAlias>()?.value?.forEach { alias ->
                            yield(alias to it.nonNullSerialName)
                        }
                    }
                }.toMap()
        }
    }
}