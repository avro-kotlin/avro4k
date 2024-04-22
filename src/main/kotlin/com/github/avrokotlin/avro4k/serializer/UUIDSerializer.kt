package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.AvroLogicalType
import com.github.avrokotlin.avro4k.AvroLogicalTypeSupplier
import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.util.UUID

object UUIDSerializer : AvroSerializer<UUID>(), AvroLogicalTypeSupplier {
    @OptIn(InternalSerializationApi::class)
    override val descriptor =
        buildSerialDescriptor("uuid", PrimitiveKind.STRING) {
            annotations = listOf(AvroLogicalType(UUIDSerializer::class))
        }

    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.uuid()
    }

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: UUID,
    ) = encoder.encodeString(obj.toString())

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ): UUID = UUID.fromString(decoder.decodeString())
}