package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.apache.avro.file.DataFileStream
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import java.io.ByteArrayOutputStream
import java.util.UUID

class AvroObjectContainerFileTest : StringSpec({
    val firstProfile =
        UserProfile(
            id = UserId(UUID.randomUUID()),
            name = "John Doe",
            age = 30,
            gender = GenderEnum.Male,
            address = null
        )
    val firstProfileGenericData =
        record(
            firstProfile.id.value.toString(),
            "John Doe",
            30,
            GenericData.EnumSymbol(Avro.schema<GenderEnum>(), "Male"),
            null
        )
    val secondProfile =
        UserProfile(
            id = UserId(UUID.randomUUID()),
            name = "Jane Doe",
            age = 25,
            gender = GenderEnum.Female,
            address = Address(city = "New York", country = "USA")
        )
    val secondProfileGenericData =
        record(
            secondProfile.id.value.toString(),
            "Jane Doe",
            25,
            GenericData.EnumSymbol(Avro.schema<GenderEnum>(), "Female"),
            record(
                "New York",
                "USA"
            )
        )

    "support writing avro object container file with metadata" {
        // write with avro4k
        val bytes =
            ByteArrayOutputStream().use {
                AvroObjectContainerFile().encodeToStream(sequenceOf(firstProfile, secondProfile), it) {
                    metadata("meta-string", "awesome string")
                    metadata("meta-long", 42)
                    metadata("bytes", byteArrayOf(1, 3, 2, 42))
                }
                it.toByteArray()
            }
        // read with apache avro lib
        val dataFile =
            bytes.inputStream().use {
                DataFileStream<GenericRecord>(it, GenericDatumReader(Avro.schema<UserProfile>()))
            }
        dataFile.getMetaString("meta-string") shouldBe "awesome string"
        dataFile.getMetaLong("meta-long") shouldBe 42
        dataFile.getMeta("bytes") shouldBe byteArrayOf(1, 3, 2, 42)
        normalizeGenericData(dataFile.next()) shouldBe firstProfileGenericData
        normalizeGenericData(dataFile.next()) shouldBe secondProfileGenericData
        dataFile.hasNext() shouldBe false
    }
    "support reading avro object container file with metadata" {
        // write with apache avro lib
        val bytes =
            ByteArrayOutputStream().use {
                val dataFileWriter = DataFileWriter(GenericDatumWriter<GenericRecord>())
                dataFileWriter.setMeta("meta-string", "awesome string")
                dataFileWriter.setMeta("meta-long", 42)
                dataFileWriter.setMeta("bytes", byteArrayOf(1, 3, 2, 42))
                dataFileWriter.create(Avro.schema<UserProfile>(), it)
                dataFileWriter.append(firstProfileGenericData.createRecord(Avro.schema<UserProfile>()))
                dataFileWriter.append(secondProfileGenericData.createRecord(Avro.schema<UserProfile>()))
                dataFileWriter.close()
                it.toByteArray()
            }
        // read with avro4k
        val profiles =
            bytes.inputStream().use {
                AvroObjectContainerFile().decodeFromStream<UserProfile>(it) {
                    metadata("meta-string")?.asString() shouldBe "awesome string"
                    metadata("meta-long")?.asLong() shouldBe 42
                    metadata("bytes")?.asBytes() shouldBe byteArrayOf(1, 3, 2, 42)
                }.toList()
            }
        profiles.size shouldBe 2
        profiles[0] shouldBe firstProfile
        profiles[1] shouldBe secondProfile
    }
}) {
    @Serializable
    private data class UserProfile(
        val id: UserId,
        val name: String,
        val age: Int,
        val gender: GenderEnum,
        val address: Address?,
    )

    @Serializable
    @AvroEnumDefault("Unknown")
    private enum class GenderEnum {
        Unknown,
        Female,
        Male,
    }

    @Serializable
    private data class Address(
        val city: String,
        val country: String,
    )

    @JvmInline
    @Serializable
    private value class UserId(
        @Contextual val value: UUID,
    )
}