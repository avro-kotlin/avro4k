package com.sksamuel.avro4k.serializers

import com.sksamuel.avro4k.FieldEncoder
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializer
import kotlinx.serialization.internal.ByteDescriptor
import kotlinx.serialization.withName
import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import java.math.BigDecimal
import java.math.RoundingMode

interface BigDecimalEncoder : FieldEncoder {

   fun encodeBigDecimal(obj: BigDecimal) {
      // we support encoding big decimals in three ways - fixed, bytes or as a String, depending on the schema passed in
      // the scale and precision should come from the schema and the rounding mode from the implicit

      val converter = Conversions.DecimalConversion()
      val rm = RoundingMode.UNNECESSARY
      val schema = fieldSchema()

      val value: Any = when (schema.type) {
         Schema.Type.STRING -> Utf8(obj.toString())
         Schema.Type.BYTES -> {
            when (val logical = schema.logicalType) {
               is LogicalTypes.Decimal -> converter.toBytes(obj.setScale(logical.scale, rm), schema, logical)
               else -> throw SerializationException("Cannot serialize BigDecimal to FIXED for logical type $logical")
            }
         }
         Schema.Type.FIXED -> {
            when (val logical = schema.logicalType) {
               is LogicalTypes.Decimal -> converter.toFixed(obj.setScale(logical.scale, rm), schema, logical)
               else -> throw SerializationException("Cannot serialize BigDecimal to FIXED for logical type $logical")
            }

         }
         else -> throw SerializationException("Cannot serialize BigDecimal as ${schema.type}")
      }

      addValue(value)
   }
}

@Serializer(forClass = BigDecimal::class)
class BigDecimalSerializer : KSerializer<BigDecimal> {

   companion object {
      const val name = "java.math.BigDecimal"
   }

   override val descriptor: SerialDescriptor = ByteDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: BigDecimal) {
      (encoder as BigDecimalEncoder).encodeBigDecimal(obj)
   }

   override fun deserialize(decoder: Decoder): BigDecimal {
      return BigDecimal(decoder.decodeString())
   }
}