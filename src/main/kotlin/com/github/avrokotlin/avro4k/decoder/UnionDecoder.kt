package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.possibleSerializationSubclasses
import com.github.avrokotlin.avro4k.schema.TypeName
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

@ExperimentalSerializationApi
class UnionDecoder(
    descriptor: SerialDescriptor,
    private val value: GenericRecord,
    override val serializersModule: SerializersModule,
    private val configuration: AvroConfiguration,
) : AbstractDecoder(), FieldDecoder {
    private enum class DecoderState(val index: Int) {
        BEFORE(0),
        READ_CLASS_NAME(1),
        READ_DONE(CompositeDecoder.DECODE_DONE),
        ;

        fun next() = values().firstOrNull { it.ordinal > this.ordinal } ?: READ_DONE
    }

    private var currentState = DecoderState.BEFORE

    private var leafDescriptor: SerialDescriptor =
        descriptor.possibleSerializationSubclasses(serializersModule).firstOrNull {
            val schemaName = TypeName(name = value.schema.name, namespace = value.schema.namespace)
            val serialName = configuration.typeNamingStrategy.resolve(it, it.serialName)
            serialName == schemaName
        } ?: throw SerializationException("Cannot find a subtype of ${descriptor.serialName} that can be used to deserialize a record of schema ${value.schema}.")

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        val currentIndex = currentState.index
        currentState = currentState.next()
        return currentIndex
    }

    override fun fieldSchema(): Schema = value.schema

    /**
     * Decode string needs to return the class name of the actual decoded class.
     */
    override fun decodeString(): String {
        return leafDescriptor.serialName
    }

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        val recordDecoder = RootRecordDecoder(value, serializersModule, configuration)
        return recordDecoder.decodeSerializableValue(deserializer)
    }

    override fun decodeAny(): Any = UnsupportedOperationException()
}