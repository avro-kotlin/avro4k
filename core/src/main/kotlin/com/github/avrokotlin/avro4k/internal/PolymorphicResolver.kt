package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.AvroAlias
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule

internal class PolymorphicResolver(private val serializersModule: SerializersModule) {
    private val cache = WeakKeyCache<SerialDescriptor, Map<String, String>>()

    fun getFullNamesAndAliasesToSerialName(descriptor: SerialDescriptor): Map<String, String> {
        return cache.getOrPut(descriptor) {
            descriptor.possibleSerializationSubclasses(serializersModule)
                .flatMap {
                    sequence {
                        yield(it.avroUnionSchemaFullName() to it.nonNullSerialName)
                        it.findAnnotation<AvroAlias>()?.value?.forEach { alias ->
                            yield(alias to it.nonNullSerialName)
                        }
                    }
                }.toMap()
        }
    }
}

/**
 * Returns the Avro schema full name that this descriptor produces when used inside a union.
 *
 * For value/inline classes, the Avro schema is the unwrapped inner type's schema, so the union
 * lookup key must match that inner schema's full name (e.g. "string" for a String-backed value class)
 * rather than the value class's own serial name.
 *
 * For all other types the serial name is used directly, matching how [ClassVisitor] and [ValueVisitor]
 * produce named Avro schemas.
 */
@OptIn(ExperimentalSerializationApi::class)
private fun SerialDescriptor.avroUnionSchemaFullName(): String {
    if (!isInline) return nonNullSerialName
    val innerDescriptor = getElementDescriptor(0)
    return when (val kind = innerDescriptor.kind) {
        is PrimitiveKind -> kind.avroTypeName()
        else -> innerDescriptor.avroUnionSchemaFullName()
    }
}

private fun PrimitiveKind.avroTypeName(): String =
    when (this) {
        PrimitiveKind.BOOLEAN -> "boolean"
        PrimitiveKind.BYTE,
        PrimitiveKind.SHORT,
        PrimitiveKind.INT,
        PrimitiveKind.CHAR,
        -> "int"
        PrimitiveKind.LONG -> "long"
        PrimitiveKind.FLOAT -> "float"
        PrimitiveKind.DOUBLE -> "double"
        PrimitiveKind.STRING -> "string"
    }