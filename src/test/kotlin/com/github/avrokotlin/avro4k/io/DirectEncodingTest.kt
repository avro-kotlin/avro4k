package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.ScalePrecision
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.util.*
import kotlin.random.Random

class DirectEncodingTest : StringSpec({

    val ennio = Composer(
        PossibleType1("a"),
        "ennio morricone",
        "rome",
        listOf(Work("legend of 1900", 1986, Color.ORANGE), Work("ecstasy of gold", 1969, Color.RED))
    )

    val hans = Composer(
        PossibleType2(17),
        "hans zimmer",
        "frankfurt",
        listOf(Work("batman begins", 2007, Color.GREEN), Work("dunkirk", 2017, Color.BLUE))
    )

    "Direct encoding should produce the same output" {
        val serializer = Composer.serializer()
        val schema = Avro.default.schema(serializer)
        val baos = ByteArrayOutputStream()
        val aos = Avro.default.openOutputStream(serializer) {
            encodeFormat = AvroEncodeFormat.Binary
            this.schema = schema
        }.to(baos)
        aos.write(ennio).write(hans)
        aos.close()
        System.gc()
        val avroLibByteArray = baos.toByteArray()
        val buffer = Buffer()
        val avroEncoder = AvroBinaryEncoder(buffer)
        Avro.default.encode(avroEncoder, serializer, ennio, schema)
        Avro.default.encode(avroEncoder, serializer, hans, schema)
        avroEncoder.flush()
        val directByteArray = buffer.readByteArray()

        directByteArray.shouldBe(avroLibByteArray)
    }
    "Encode all possible variations" {
        val varations = SchemaVariations(
            "ennio morricone",
            "rome",
            listOf(Work("legend of 1900", 1986, Color.ORANGE), Work("ecstasy of gold", 1969, Color.RED)),
            PossibleType1("name")
        )
        val serializer = SchemaVariations.serializer()
        val schema = Avro.default.schema(serializer)
        val buffer = Buffer()
        val avroEncoder = AvroBinaryEncoder(buffer)
        Avro.default.encode(avroEncoder, serializer, varations, schema)
        val byteArray = buffer.readByteArray()
        byteArray.isNotEmpty() shouldBe true

    }
    "Encode polymorphic" {
        val buffer = Buffer()
        val avroEncoder = AvroBinaryEncoder(buffer)
        Avro.default.encode(avroEncoder, Union.serializer(), PossibleType1("name"), Avro.default.schema(Union.serializer()))
        avroEncoder.flush()
        buffer.readByteArray().isEmpty() shouldBe false 
    }
    "Encode primitives" {
        val buffer = Buffer()
        val avroEncoder = AvroBinaryEncoder(buffer)
        Avro.default.encode(avroEncoder, Long.serializer(), 2L, Avro.default.schema(Long.serializer()))
        avroEncoder.flush()
        buffer.readByteArray() shouldBe byteArrayOf(4)
    }
    "Encoding of Clients should work" {
        val serializer = Clients.serializer()
        val schema = Avro.default.schema(serializer)
        val buffer = Buffer()
        val avroEncoder = AvroBinaryEncoder(buffer)
        val clients = Clients()
        clients.clients.add(
            Client(
                id = 9852,
                index = 7079,
                guid = UUID.randomUUID(),
                isActive = false,
                balance = BigDecimal.valueOf(123.1244),
                picture = Random.nextBytes(1024),
                age = 4615,
                eyeColor = EyeColor.BROWN,
                name = "My Name",
                gender = "my gender",
                company = "my company",
                emails = arrayOf("email"),
                phones = longArrayOf(12344L),
                address = "addr",
                about = "about",
                registered = LocalDate.now(),
                latitude = 4.5,
                longitude = 6.7,
                tags = listOf("Tag1"),
                partners = listOf()
            )
        )
        Avro.default.encode(avroEncoder, serializer, clients, schema)
    }

}) {
    @Serializable
    enum class Color { RED, BLUE, GREEN, ORANGE }

    @Serializable
    data class Work(val name: String, val year: Int, val color: Color)

    @Serializable
    data class Composer(
        val union: Union,
        val name: String,
        val birthplace: String,
        val works: List<Work>
    )

    @Serializable
    data class SchemaVariations(
        val name: String,
        val birthplace: String,
        val works: List<Work>,
        val union: Union,
        val nullableReference: Work? = works.first(),
        val nullableList: List<Work>? = works,
        val nullableMap: Map<String, Work>? = works.associateBy { it.name },
        val nullableMapValue: Map<String, Work?> = works.associateBy { it.name } + mapOf("work" to null),
        val binary: ByteArray = byteArrayOf(0x01, 0x02, 0x03),
        @AvroFixed(4)
        val fixedByteArray: ByteArray = byteArrayOf(0x01, 0x78, 0x60, 0x22),
        val mapOfWorks: Map<String, Work> = works.associateBy { it.name },
        val nullableListElements: List<Work?> = works + null,
        val doubleValue: Double = 1.023,
        val floatValue: Float = 2.323f,
        val booleanValue: Boolean = true,
        val nullableBoolean: Boolean? = true,
        val nullableBooleanWithNull: Boolean? = null,
        val nullableInt: Int? = 1,
        val nullableIntWithNull: Int? = null,
        val nullableDouble: Double? = 2.44,
        val nullableDoubleWithNull: Double? = null,
        val nullableFloat: Float? = 1.233f,
        val nullableFloatWithNull: Float? = null,
        val nullableString: String? = "blub",
        val nullableStringWithNull: String? = null
    )

    @Serializable
    sealed interface Union
    @Serializable
    data class PossibleType1(
        val name: String
    ) : Union

    @Serializable
    data class PossibleType2(
        val age: Int
    ) : Union

    @Serializable
    data class Clients(
        var clients: MutableList<Client> = mutableListOf()
    )

    @Serializable
    data class Client(
        var id: Long = 0,
        var index: Int = 0,
        @Serializable(with = UUIDSerializer::class)
        var guid: UUID? = null,
        var isActive: Boolean = false,
        @Serializable(with = BigDecimalSerializer::class)
        @ScalePrecision(5, 10)
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
        @Serializable(with = LocalDateSerializer::class)
        var registered: LocalDate? = null,
        var latitude: Double = 0.0,
        var longitude: Double = 0.0,
        var tags: List<String> = emptyList(),
        var partners: List<Partner> = emptyList(),
    )

    @Serializable
    enum class EyeColor {
        BROWN,
        BLUE,
        GREEN;
    }

    @Serializable
    class Partner(
        val id: Long = 0,
        val name: String? = null,
        @Serializable(with = InstantSerializer::class)
        val since: Instant? = null
    )

}
