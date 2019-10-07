package com.sksamuel.avro4k.serializer

import com.sksamuel.avro4k.AnnotationExtractor
import com.sksamuel.avro4k.encoder.FieldEncoder
import com.sksamuel.avro4k.schema.AvroDescriptor
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializer
import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.util.Utf8
import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.reflect.jvm.jvmName

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

interface BigDecimalDecoder {
   fun decodeBigDecimal(): BigDecimal
}

@Serializer(forClass = BigDecimal::class)
class BigDecimalSerializer : KSerializer<BigDecimal> {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(BigDecimal::class.jvmName, PrimitiveKind.BYTE) {
      override fun schema(annos: List<Annotation>): Schema {
         val schema = SchemaBuilder.builder().bytesType()
         val (scale, precision) = AnnotationExtractor(annos).scalePrecision() ?: 2 to 8
         return LogicalTypes.decimal(precision, scale).addToSchema(schema)
      }
   }

   override fun serialize(encoder: Encoder, obj: BigDecimal) {
      (encoder as BigDecimalEncoder).encodeBigDecimal(obj)
   }

   override fun deserialize(decoder: Decoder): BigDecimal {
      return (decoder as BigDecimalDecoder).decodeBigDecimal()
   }
}