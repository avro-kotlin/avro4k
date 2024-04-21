package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.SerializationException
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

object StringFromAvroValue {
    fun fromValue(value: Any?): String {
        return when (value) {
            is CharSequence -> value.toString()
            is GenericData.Fixed -> String(value.bytes())
            is ByteArray -> String(value)
            is ByteBuffer -> String(value.array())
            null -> throw SerializationException("Cannot decode <null> as a string")
            else -> throw SerializationException("Unsupported type for String [is ${value::class.qualifiedName}]")
        }
    }
}