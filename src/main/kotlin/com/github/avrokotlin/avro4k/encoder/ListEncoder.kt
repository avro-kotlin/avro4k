package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData

@ExperimentalSerializationApi
class ListEncoder(
        listSchema: Schema,
        size: Int,
        override val serializersModule: SerializersModule,
        override val configuration: AvroConfiguration,
        private val callback: (GenericArray<Any?>) -> Unit,
) : AvroStructureEncoder() {
    override val currentUnresolvedSchema: Schema = listSchema.elementType

    private val list = GenericData.Array<Any?>(size, listSchema)

    override fun encodeNativeValue(value: Any?) {
        list.add(value)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        callback(list)
    }
}