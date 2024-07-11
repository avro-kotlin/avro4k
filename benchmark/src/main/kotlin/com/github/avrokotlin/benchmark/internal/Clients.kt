package com.github.avrokotlin.benchmark.internal

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.github.avrokotlin.avro4k.AvroStringable
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate

@Serializable
internal data class Clients(
    val clients: List<Client>
)

class CharJacksonSerializer : StdSerializer<Char>(Char::class.java) {
    override fun serialize(value: Char?, gen: JsonGenerator, provider: SerializerProvider) {
        value?.code?.let { gen.writeNumber(it) } ?: gen.writeNull()
    }

    override fun acceptJsonFormatVisitor(visitor: JsonFormatVisitorWrapper, typeHint: JavaType) {
        visitor.expectIntegerFormat(typeHint).numberType(JsonParser.NumberType.INT)
    }
}

class CharJacksonDeserializer : StdDeserializer<Char>(Char::class.java) {
    override fun deserialize(p0: JsonParser, p1: com.fasterxml.jackson.databind.DeserializationContext): Char {
        return p0.intValue.toChar()
    }
}

@Serializable
internal data class Client(
    val id: Long,
    val index: Int,
    val isActive: Boolean,
    @Contextual
    @AvroStringable
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    val balance: BigDecimal?,
    val picture: ByteArray?,
    val age: Int,
    val eyeColor: EyeColor?,
    val name: String?,
    @JsonSerialize(using = CharJacksonSerializer::class)
    @JsonDeserialize(using = CharJacksonDeserializer::class)
    val gender: Char?,
    val company: String?,
    val emails: Array<String>,
    val phones: LongArray,
    val address: String?,
    val about: String?,
    @Serializable(with = LocalDateSerializer::class)
    val registered: LocalDate?,
    val latitude: Double,
    val longitude: Float,
    val tags: List<String?>,
    val partner: Partner?,
    val map: Map<String, String>,
)

@Serializable
internal enum class EyeColor {
    BROWN,
    BLUE,
    GREEN;
}

@Serializable
sealed interface Partner

@Serializable
internal class GoodPartner(
    val id: Long,
    val name: String,
    @Serializable(with = InstantSerializer::class)
    val since: Instant
) : Partner

@Serializable
internal class BadPartner(
    val id: Long,
    val name: String,
    @Serializable(with = InstantSerializer::class)
    val since: Instant
) : Partner

@Serializable
internal enum class Stranger : Partner {
    KNOWN_STRANGER,
    UNKNOWN_STRANGER
}
