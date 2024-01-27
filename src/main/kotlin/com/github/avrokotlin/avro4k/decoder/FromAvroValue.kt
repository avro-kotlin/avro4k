package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.SerializationException
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

interface FromAvroValue<T, R> {
   fun fromValue(value: T): R
}

object StringFromAvroValue : FromAvroValue<Any?, String> {
   override fun fromValue(value: Any?): String {
      return when (value) {
         is String -> value
         is Utf8 -> value.toString()
         is GenericData.Fixed -> String(value.bytes())
         is ByteArray -> String(value)
         is CharSequence -> value.toString()
         is ByteBuffer -> String(value.array())
         null -> throw SerializationException("Cannot decode <null> as a string")
         else -> throw SerializationException("Unsupported type for String [is ${value::class.qualifiedName}]")
      }
   }
}

object EnumFromAvroValue : FromAvroValue<Any, String> {
   override fun fromValue(value: Any): String {
      return when (value) {
         is GenericEnumSymbol<*> -> value.toString()
         is String -> value
         else -> value.toString()
      }
   }
}
