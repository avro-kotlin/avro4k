package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnnotationExtractor
import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.avro.ExtendedEncoder
import com.github.avrokotlin.avro4k.schema.AvroDescriptor
import com.github.avrokotlin.avro4k.schema.NamingStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode
import kotlin.reflect.jvm.jvmName

@OptIn(ExperimentalSerializationApi::class)
class BigDecimalSerializer : AvroSerializer<BigDecimal>() {
   override fun decodeAvroValue(readSchema: Schema, decoder: ExtendedDecoder): BigDecimal {
      fun logical() = when (val l = readSchema.logicalType) {
         is LogicalTypes.Decimal -> l
         else -> throw SerializationException("Cannot decode to BigDecimal when field schema [$readSchema] does not define Decimal logical type [$l]")
      }
      return when(readSchema.type) {
         Schema.Type.STRING -> BigDecimal(decoder.decodeString())
         Schema.Type.BYTES -> createFromByteArray(decoder.decodeBytes(), logical())
         Schema.Type.FIXED -> createFromByteArray(decoder.decodeFixed(), logical())
         else -> throw SerializationException("The schema type ${readSchema.type} is no supported type for the logical type 'Decimal'")
      }
   }
   fun createFromByteArray(byteArray: ByteArray, logicalType: LogicalTypes.Decimal) : BigDecimal {
      val scale = logicalType.scale
      return BigDecimal(BigInteger(byteArray), scale)
   }

   override val descriptor: SerialDescriptor = object : AvroDescriptor(BigDecimal::class.jvmName, PrimitiveKind.BYTE) {
      override fun schema(annos: List<Annotation>, serializersModule: SerializersModule, namingStrategy: NamingStrategy): Schema {
         val schema = SchemaBuilder.builder().bytesType()
         val (scale, precision) = AnnotationExtractor(annos).scalePrecision() ?: (2 to 8)
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
}