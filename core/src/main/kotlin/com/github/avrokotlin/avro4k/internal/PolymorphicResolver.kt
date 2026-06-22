package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.AvroAlias
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule

internal class PolymorphicResolver(
    private val serializersModule: SerializersModule,
    private val schemaNameResolver: (SerialDescriptor) -> String,
) {
    private val cache = WeakKeyCache<SerialDescriptor, Map<String, String>>()

    fun getFullNamesAndAliasesToSerialName(descriptor: SerialDescriptor): Map<String, String> {
        return cache.getOrPut(descriptor) {
            descriptor.possibleSerializationSubclasses(serializersModule)
                .flatMap {
                    sequence {
                        yield(schemaNameResolver(it) to it.nonNullSerialName)
                        it.findAnnotation<AvroAlias>()?.value?.forEach { alias ->
                            yield(alias to it.nonNullSerialName)
                        }
                    }
                }.toMap()
        }
    }
}