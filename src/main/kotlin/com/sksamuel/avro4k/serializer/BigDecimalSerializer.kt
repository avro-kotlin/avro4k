package com.sksamuel.avro4k.serializer

import com.sksamuel.avro4k.AnnotationExtractor
import com.sksamuel.avro4k.decoder.ExtendedDecoder
import com.sksamuel.avro4k.encoder.ExtendedEncoder
import com.sksamuel.avro4k.schema.AvroDescriptor
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializer
import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8
import java.math.BigDecimal
import java.math.RoundingMode
import java.nio.ByteBuffer
import kotlin.reflect.jvm.jvmName

@Serializer(forClass = BigDecimal::class)
class BigDecimalSerializer : AvroSerializer<BigDecimal>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(BigDecimal::class.jvmName, PrimitiveKind.BYTE) {
      override fun schema(annos: List<Annotation>): Schema {
         val schema = SchemaBuilder.builder().bytesType()
         val (scale, precision) = AnnotationExtractor(annos).scalePrecision() ?: 2 to 8
         return LogicalTypes.decimal(precision, scale).addToSchema(schema)
      }
   }

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: BigDecimal) {

      // we support encoding big decimals in three ways - fixed, bytes or as a String, depending on the schema passed in
      // the scale and precision should come from the schema and the rounding mode from the implicit

      val converter = Conversions.DecimalConversion()
      val rm = RoundingMode.UNNECESSARY

      return when (schema.type) {
         Schema.Type.STRING -> encoder.encodeString(obj.toString())
         Schema.Type.BYTES -> {
            when (val logical = schema.logicalType) {
               is LogicalTypes.Decimal -> encoder.encodeByteArray(converter.toBytes(obj.setScale(logical.scale, rm),
                  schema,
                  logical))
               else -> throw SerializationException("Cannot encode BigDecimal to FIXED for logical type $logical")
            }
         }
         Schema.Type.FIXED -> {
            when (val logical = schema.logicalType) {
               is LogicalTypes.Decimal -> encoder.encodeFixed(converter.toFixed(obj.setScale(logical.scale, rm),
                  schema,
                  logical))
               else -> throw SerializationException("Cannot encode BigDecimal to FIXED for logical type $logical")
            }

         }
         else -> throw SerializationException("Cannot encode BigDecimal as ${schema.type}")
      }
   }

   override fun fromAvroValue(schema: Schema, decoder: ExtendedDecoder): BigDecimal {

      fun logical() = when (val l = schema.logicalType) {
         is LogicalTypes.Decimal -> l
         else -> throw SerializationException("Cannot decode to BigDecimal when field schema [$schema] does not define Decimal logical type [$l]")
      }

      return when (val v = decoder.decodeAny()) {
         is Utf8 -> BigDecimal(decoder.decodeString())
         is ByteArray -> Conversions.DecimalConversion().fromBytes(ByteBuffer.wrap(v), schema, logical())
         is ByteBuffer -> Conversions.DecimalConversion().fromBytes(v, schema, logical())
         is GenericFixed -> Conversions.DecimalConversion().fromFixed(v, schema, logical())
         else -> throw SerializationException("Unsupported BigDecimal type [$v]")
      }
   }
}