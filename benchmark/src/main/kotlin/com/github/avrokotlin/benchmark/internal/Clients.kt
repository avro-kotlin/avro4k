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
import com.github.avrokotlin.avro4k.serializer.BigDecimalAsStringSerializer
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
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
    val id: Long = 0,
    val index: Int = 0,
    val isActive: Boolean = false,
    @Serializable(with = BigDecimalAsStringSerializer::class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    val balance: BigDecimal? = null,
    val picture: ByteArray? = null,
    val age: Int = 0,
    val eyeColor: EyeColor? = null,
    val name: String? = null,
    @JsonSerialize(using = CharJacksonSerializer::class)
    @JsonDeserialize(using = CharJacksonDeserializer::class)
    val gender: Char? = null,
    val company: String? = null,
    val emails: Array<String> = emptyArray(),
    val phones: LongArray = LongArray(0),
    val address: String? = null,
    val about: String? = null,
    @Serializable(with = LocalDateSerializer::class)
    val registered: LocalDate? = null,
    val latitude: Double = 0.0,
    val longitude: Float = 0.0f,
    val tags: List<String?> = emptyList(),
    val partner: Partner,
    val map: Map<String, String> = emptyMap(),
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
    val id: Long = 0,
    val name: String? = null,
    @Serializable(with = InstantSerializer::class)
    val since: Instant? = null
) : Partner

@Serializable
internal class BadPartner(
    val id: Long = 0,
    val name: String? = null,
    @Serializable(with = InstantSerializer::class)
    val since: Instant? = null
) : Partner

@Serializable
internal enum class Stranger : Partner {
    KNOWN_STRANGER,
    UNKNOWN_STRANGER
}
