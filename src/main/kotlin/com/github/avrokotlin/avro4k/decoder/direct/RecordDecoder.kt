package com.github.avrokotlin.avro4k.decoder.direct

import com.github.avrokotlin.avro4k.io.AvroDecoder
import com.github.avrokotlin.avro4k.schema.Resolver
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class RecordDecoder(
    override val decoder: AvroDecoder,
    override val serializersModule: SerializersModule,
    private val resolvedAction: Resolver.RecordAdjust
) : StructureDecoder() {
    private var currentWriterFieldIndex = 0
    private var currentReaderFieldIndex = 0
    private lateinit var currentFieldAction: Resolver.Action
    
    override val currentAction: Resolver.Action
        get() = currentFieldAction
    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        while (currentWriterFieldIndex < resolvedAction.fieldActions.size && resolvedAction.fieldActions[currentWriterFieldIndex] is Resolver.Skip) {
            currentWriterFieldIndex++
            val skipAction = resolvedAction.fieldActions[currentWriterFieldIndex] as Resolver.Skip
            skip(skipAction.writer)
        }
        return if (currentWriterFieldIndex < resolvedAction.fieldActions.size) {
            //Decode from writer
            val readField = resolvedAction.readerOrder[currentReaderFieldIndex++]
            currentFieldAction = resolvedAction.fieldActions[currentWriterFieldIndex]
            readField.pos()
        } else {
            CompositeDecoder.DECODE_DONE
        }
    }
    private fun skip(schema: Schema) {
        when (schema.type) {
            Schema.Type.RECORD -> for (field in schema.fields) skip(field.schema())
            Schema.Type.ENUM -> decoder.readEnum()
            Schema.Type.ARRAY -> {
                val elementType = schema.elementType
                var l = decoder.skipArray()
                while (l > 0) {
                    var i: Long = 0
                    while (i < l) {
                        skip(elementType)
                        i++
                    }
                    l = decoder.skipArray()
                }
            }

            Schema.Type.MAP -> {
                val value = schema.valueType
                var l = decoder.skipMap()
                while (l > 0) {
                    var i: Long = 0
                    while (i < l) {
                        decoder.skipString()
                        skip(value)
                        i++
                    }
                    l = decoder.skipMap()
                }
            }

            Schema.Type.UNION -> skip(schema.types[decoder.readIndex()])
            Schema.Type.FIXED -> decoder.skipFixed(schema.fixedSize)
            Schema.Type.STRING -> decoder.skipString()
            Schema.Type.BYTES -> decoder.skipBytes()
            Schema.Type.INT -> decoder.readInt()
            Schema.Type.LONG -> decoder.readLong()
            Schema.Type.FLOAT -> decoder.readFloat()
            Schema.Type.DOUBLE -> decoder.readDouble()
            Schema.Type.BOOLEAN -> decoder.readBoolean()
            Schema.Type.NULL -> decoder.readNull()
            else -> throw RuntimeException("Unknown type: $schema")
        }
    }
}