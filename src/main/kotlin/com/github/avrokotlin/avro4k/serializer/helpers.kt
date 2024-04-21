package com.github.avrokotlin.avro4k.serializer

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.buildSerialDescriptor

@OptIn(InternalSerializationApi::class, ExperimentalSerializationApi::class)
fun buildByteArraySerialDescriptor(
    serialName: String,
    vararg annotations: Annotation,
) = buildSerialDescriptor(serialName, StructureKind.LIST) {
    element("item", buildSerialDescriptor("item", PrimitiveKind.BYTE))
    this.annotations = listOf(*annotations)
}

fun Long.toIntExact(): Int {
    return Math.toIntExact(this)
}