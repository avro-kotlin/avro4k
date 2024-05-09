package com.github.avrokotlin.benchmark

import com.github.avrokotlin.avro4k.AvroDecimal
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.util.*

@Serializable
internal data class Clients(
    var clients: MutableList<Client> = mutableListOf()
)
@Serializable
internal data class Client(
    var id: Long = 0,
    var index: Int = 0,
    @Contextual
    var guid: UUID? = null,
    var isActive: Boolean = false,
    @Contextual
    @AvroDecimal(5,10)
    var balance: BigDecimal? = null,
    var picture: ByteArray? = null,
    var age: Int = 0,
    var eyeColor: EyeColor? = null,
    var name: String? = null,
    var gender: String? = null,
    var company: String? = null,
    var emails: Array<String> = emptyArray(),
    var phones: LongArray = LongArray(0),
    var address: String? = null,
    var about: String? = null,
    @Contextual
    var registered: LocalDate? = null,
    var latitude : Double = 0.0,
    var longitude: Double = 0.0,
    var tags: List<String> = emptyList(),
    var partners: List<Partner> = emptyList(),
)

@Serializable
internal enum class EyeColor {
    BROWN,
    BLUE,
    GREEN;
}
@Serializable
internal class Partner(
    val id: Long = 0,
    val name: String? = null,
    @Contextual
    val since: Instant? = null
)
