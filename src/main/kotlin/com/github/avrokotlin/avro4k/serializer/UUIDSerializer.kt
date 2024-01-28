package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AvroUuidLogicalType
import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import org.apache.avro.Schema
import java.util.UUID

@OptIn(ExperimentalSerializationApi::class)
@Serializer(forClass = UUID::class)
class UUIDSerializer : AvroSerializer<UUID>() {
    private val avroUuidLogicalTypeAnnotation = AvroUuidLogicalType()

    @OptIn(InternalSerializationApi::class)
    override val descriptor =
        buildSerialDescriptor("uuid", PrimitiveKind.STRING) {
            annotations = listOf(avroUuidLogicalTypeAnnotation)
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