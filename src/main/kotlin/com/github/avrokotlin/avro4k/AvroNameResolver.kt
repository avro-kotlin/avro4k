package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.DefaultNamingStrategy
import com.github.avrokotlin.avro4k.schema.NamingStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import java.util.concurrent.ConcurrentHashMap

@OptIn(ExperimentalSerializationApi::class)
class AvroNameResolver(private val avro: Avro) {
    private val typeNameByDescriptor = ConcurrentHashMap<SerialDescriptor, RecordNaming>()
    private val elementNamesByDescriptor = ConcurrentHashMap<SerialDescriptor, Array<RecordNaming>>()
    private val elementNamesByIndexesByDescriptor = ConcurrentHashMap<SerialDescriptor, Map<String, Int>>()

    fun resolveTypeName(descriptor: SerialDescriptor): RecordNaming {
        return typeNameByDescriptor.getOrPut(descriptor) {
            getAvroName(
                if (descriptor.isNullable) descriptor.serialName.removeSuffix('?') else descriptor.serialName,
                descriptor.annotations,
                DefaultNamingStrategy,
            )
        }
    }

    fun resolveElementName(descriptor: SerialDescriptor, elementIndex: Int): RecordNaming {
        return elementNamesByDescriptor.getOrPut(descriptor) {
            Array(descriptor.elementsCount) { idx ->
                getAvroName(
                    descriptor.getElementName(idx),
                    descriptor.getElementAnnotations(idx),
                    avro.configuration.namingStrategy,
                )
            }
        }[elementIndex]
    }

    fun resolveElementIndex(descriptor: SerialDescriptor, elementName: String): Int? {
        return elementNamesByIndexesByDescriptor.getOrPut(descriptor) {
            (0 until descriptor.elementsCount).associateBy { avro.nameResolver.resolveElementName(descriptor, it).name }
        }[elementName]
    }
}

private fun getAvroName(
    serialName: String,
    annotations: List<Annotation>,
    namingStrategy: NamingStrategy,
): RecordNaming {
    val splitName = splitSerialName(serialName)
    val namespaceOverride = annotations.firstIsInstanceOrNull<AvroNamespace>()?.value
    val nameOverride = annotations.firstIsInstanceOrNull<AvroName>()?.value

    if (nameOverride == null && namespaceOverride == null && namingStrategy === DefaultNamingStrategy) {
        return splitName
    }
    return RecordNaming(
        name = namingStrategy.to(nameOverride ?: splitName.name),
        namespace = namespaceOverride ?: splitName.namespace,
    )
}

private fun splitSerialName(serialName: String): RecordNaming {
    val dotIndex = serialName.lastIndexOf('.').takeIf { it >= 0 }
    val namespace = dotIndex?.let { serialName.substring(0, it) }
    val name = dotIndex?.let { serialName.substring(it + 1) } ?: serialName

    return RecordNaming(
        name = name,
        namespace = namespace,
    )
}

internal fun String.removeSuffix(char: Char): String =
    if (endsWith(char)) substring(0, length - 1) else this
