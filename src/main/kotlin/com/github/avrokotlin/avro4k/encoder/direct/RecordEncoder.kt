package com.github.avrokotlin.avro4k.encoder.direct

import com.github.avrokotlin.avro4k.AnnotationExtractor
import com.github.avrokotlin.avro4k.io.AvroEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class RecordEncoder(
    private val schema: Schema, override val serializersModule: SerializersModule, override val avroEncoder: AvroEncoder
) : StructureEncoder() {

    private var currentIndex = -1
    private lateinit var fieldSchema: Schema
    override fun fieldSchema(): Schema = fieldSchema

    override fun encodeElement(descriptor: SerialDescriptor, index: Int): Boolean {
        currentIndex = index
        fieldSchema = schema.fields[currentIndex].schema()
        return true
    }
    
    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        // if we have a value type, then we don't want to begin a new structure
        return if (AnnotationExtractor(descriptor.annotations).valueType()) this
        else super.beginStructure(descriptor)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        // no op
    }
}
