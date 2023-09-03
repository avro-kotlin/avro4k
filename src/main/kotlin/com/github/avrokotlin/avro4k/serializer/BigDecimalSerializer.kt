package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnnotationExtractor
import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.NativeAvroEncoder
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
import org.apache.avro.generic.GenericFixed
import java.math.BigDecimal
import java.nio.ByteBuffer
import kotlin.reflect.jvm.jvmName

/**
 * A BigDecimal is serialized in 3 ways:
 * - as a [String] when schema type is [Schema.Type.STRING] (scale and precision have not effect here)
 * - as a [ByteBuffer] when schema type is [Schema.Type.BYTES] (see [Conversions.DecimalConversion.toBytes])
 * - as a [GenericFixed] when schema type is [Schema.Type.FIXED] (see [Conversions.DecimalConversion.toFixed])
 */
@OptIn(ExperimentalSerializationApi::class)
object BigDecimalSerializer : AvroSerializer<BigDecimal>() {
    private val converter = Conversions.DecimalConversion()

    override val descriptor: SerialDescriptor = object : AvroDescriptor(BigDecimal::class.jvmName, PrimitiveKind.BYTE) {
        override fun schema(annos: List<Annotation>, serializersModule: SerializersModule, namingStrategy: NamingStrategy): Schema {
            val schema = SchemaBuilder.builder().bytesType()
            val (scale, precision) = AnnotationExtractor(annos).scalePrecision() ?: (2 to 8)
            return LogicalTypes.decimal(precision, scale).addToSchema(schema)
        }
    }

    override fun encodeAvroValue(schema: Schema, encoder: NativeAvroEncoder, obj: BigDecimal) {
        when (schema.type) {
            Schema.Type.STRING -> encoder.encodeString(obj.toString())

            Schema.Type.BYTES -> when (val logical = schema.logicalType) {
                is LogicalTypes.Decimal -> encoder.encodeBytes(converter.toBytes(obj.setScale(logical.scale), schema, logical))
                else -> throw SerializationException("Cannot encode BigDecimal to FIXED for logical type $logical")
            }

            Schema.Type.FIXED -> when (val logical = schema.logicalType) {
                is LogicalTypes.Decimal -> encoder.encodeFixed(converter.toFixed(obj.setScale(logical.scale), schema, logical))
                else -> throw SerializationException("Cannot encode BigDecimal to FIXED for logical type $logical")
            }

            else -> throw SerializationException("Cannot encode BigDecimal as ${schema.type}")
        }
    }

    override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): BigDecimal {
        fun logical() = when (val l = schema.logicalType) {
            is LogicalTypes.Decimal -> l
            else -> throw SerializationException("Cannot decode to BigDecimal when field schema [$schema] does not define Decimal logical type [$l]")
        }

        return when (val value = decoder.decodeAny()) {
            is CharSequence -> value.toString().toBigDecimal()
            is ByteArray -> converter.fromBytes(ByteBuffer.wrap(value), schema, logical())
            is ByteBuffer -> converter.fromBytes(value, schema, logical())
            is GenericFixed -> converter.fromFixed(value, schema, logical())
            else -> throw SerializationException("Unsupported BigDecimal type [$value]")
        }
    }
}
