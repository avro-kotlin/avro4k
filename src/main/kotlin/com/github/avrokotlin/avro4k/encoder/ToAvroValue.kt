package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.schema.extractNonNull
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

object StringToAvroValue {
   fun toValue(schema: Schema, t: String): Any {
      return when (schema.type) {
         Schema.Type.FIXED -> {
            val size = t.toByteArray().size
            if (size > schema.fixedSize)
               throw AvroRuntimeException("Cannot write string with $size bytes to fixed type of size ${schema.fixedSize}")
            // the array passed in must be padded to size
            val bytes = ByteBuffer.allocate(schema.fixedSize).put(t.toByteArray()).array()
            GenericData.get().createFixed(null, bytes, schema)
         }
         Schema.Type.BYTES -> ByteBuffer.wrap(t.toByteArray())
         else -> Utf8(t)
      }
   }
}
@ExperimentalSerializationApi
object ValueToEnum {
   fun toValue(schema: Schema, enumDescription: SerialDescriptor, ordinal: Int): GenericData.EnumSymbol {
      // the schema provided will be a union, so we should extract the correct schema
      val symbol = enumDescription.getElementName(ordinal)
      val nonNullSchema = schema.extractNonNull()
      return GenericData.get().createEnum(symbol, nonNullSchema) as GenericData.EnumSymbol
   }
}