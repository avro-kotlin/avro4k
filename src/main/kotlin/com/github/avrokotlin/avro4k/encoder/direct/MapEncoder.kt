package com.github.avrokotlin.avro4k.encoder.direct


import com.github.avrokotlin.avro4k.io.AvroEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class MapEncoder(
    schema: Schema,
    override val serializersModule: SerializersModule,
    override val avroEncoder: AvroEncoder,
    size: Int
) : StructureEncoder(),
    CompositeEncoder {

    private var key: String? = null
    private val valueSchema = schema.valueType

    override fun fieldSchema(): Schema = valueSchema

    init {
        avroEncoder.writeMapStart(size)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        avroEncoder.writeMapEnd()
    }


    override fun encodeString(value: String) {
        val k = key
        if (k == null) {
            key = value
            avroEncoder.startItem()
            avroEncoder.writeString(value)
        } else {
            super.encodeString(value)
        }
    }

    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        val wasNotNull = key != null
        super.encodeSerializableValue(serializer, value)
        //A value has been encoded, set key to null again
        if (wasNotNull) key = null
    }
}
