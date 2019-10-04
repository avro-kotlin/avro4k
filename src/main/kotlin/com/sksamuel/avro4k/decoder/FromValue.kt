package com.sksamuel.avro4k.decoder

import kotlinx.serialization.SerializationException
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

interface FromValue<T> {
   fun fromValue(value: Any?): T
}

object StringFromValue : FromValue<String> {
   override fun fromValue(value: Any?): String {
      return when (value) {
         is String -> value
         is Utf8 -> value.toString()
         is GenericData.Fixed -> String(value.bytes())
         is ByteArray -> String(value)
         is CharSequence -> value.toString()
         is ByteBuffer -> String(value.array())
         null -> throw SerializationException("Cannot decode <null> as a string")
         else -> throw SerializationException("Unsupported type for String ${value.javaClass}")
      }
   }
}
