package com.github.avrokotlin.avro4k.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class EnumEncoder (private val schema: Schema,
override val serializersModule: SerializersModule,
private val callback: (Any) -> Unit) : AbstractEncoder() {

    private var enumValue : GenericData.EnumSymbol? = null

    @OptIn(ExperimentalSerializationApi::class)
    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        enumValue = ValueToEnum.toValue(schema, enumDescriptor, index)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        when (schema.type) {
            Schema.Type.ENUM -> {
                callback(enumValue!!)
            }
            else -> throw SerializationException("Cannot encode byte array when schema is ${schema.type}")
        }

    }
}