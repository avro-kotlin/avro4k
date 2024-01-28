@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.LogicalTypes
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.util.UUID

class UUIDIoTest : StringSpec({

    "read / write UUID" {

        val uuid = UUID.randomUUID()

        writeRead(UUIDTest(uuid), UUIDTest.serializer())
        writeRead(UUIDTest(uuid), UUIDTest.serializer()) {
            it["a"] shouldBe Utf8(uuid.toString())
        }
    }

    "read / write list of UUIDs" {

        val uuid1 = UUID.randomUUID()
        val uuid2 = UUID.randomUUID()

        writeRead(UUIDListTest(listOf(uuid1, uuid2)), UUIDListTest.serializer())
        writeRead(UUIDListTest(listOf(uuid1, uuid2)), UUIDListTest.serializer()) {
            val uuidSchema = SchemaBuilder.builder().stringType()
            LogicalTypes.uuid().addToSchema(uuidSchema)
            val schema = SchemaBuilder.array().items(uuidSchema)
            it["a"] shouldBe GenericData.Array(schema, listOf(Utf8(uuid1.toString()), Utf8(uuid2.toString())))
        }
    }

    "read / write nullable UUIDs" {

        val uuid = UUID.randomUUID()

        writeRead(NullableUUIDTest(uuid), NullableUUIDTest.serializer()) {
            it["a"] shouldBe Utf8(uuid.toString())
        }

        writeRead(NullableUUIDTest(null), NullableUUIDTest.serializer()) {
            it["a"] shouldBe null
        }
    }
}) {
    @Serializable
    data class UUIDTest(val a: UUID)

    @Serializable
    data class UUIDListTest(val a: List<UUID>)

    @Serializable
    data class NullableUUIDTest(val a: UUID?)
}