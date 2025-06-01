package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.AvroEnumDefault
import kotlinx.serialization.descriptors.SerialDescriptor
import java.util.WeakHashMap

internal class EnumResolver {
    private val defaultIndexCache: MutableMap<SerialDescriptor, EnumDefault> = WeakHashMap()

    private data class EnumDefault(val index: Int?)

    fun getDefaultValueIndex(enumDescriptor: SerialDescriptor): Int? {
        return defaultIndexCache.getOrPut(enumDescriptor) {
            loadCache(enumDescriptor)
        }.index
    }

    private fun loadCache(enumDescriptor: SerialDescriptor): EnumDefault {
        var foundIndex: Int? = null
        for (i in 0 until enumDescriptor.elementsCount) {
            if (enumDescriptor.getElementAnnotations(i).any { it is AvroEnumDefault }) {
                if (foundIndex != null) {
                    throw UnsupportedOperationException("Multiple default values found in enum $enumDescriptor")
                }
                foundIndex = i
            }
        }
        return EnumDefault(foundIndex)
    }
}