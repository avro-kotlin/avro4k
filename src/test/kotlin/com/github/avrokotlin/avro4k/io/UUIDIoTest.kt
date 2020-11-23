@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.LogicalTypes
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.util.*

class UUIDIoTest : StringSpec({

   "read / write UUID" {

      @Serializable
      data class UUIDTest(val a: UUID)

      val uuid = UUID.randomUUID()

      writeRead(UUIDTest(uuid), UUIDTest.serializer())
      writeRead(UUIDTest(uuid), UUIDTest.serializer()) {
         it["a"] shouldBe Utf8(uuid.toString())
      }
   }

   "read / write list of UUIDs" {

      @Serializable
      data class UUIDTest(val a: List<UUID>)

      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()

      writeRead(UUIDTest(listOf(uuid1, uuid2)), UUIDTest.serializer())
      writeRead(UUIDTest(listOf(uuid1, uuid2)), UUIDTest.serializer()) {
         val uuidSchema = SchemaBuilder.builder().stringType()
         LogicalTypes.uuid().addToSchema(uuidSchema)
         val schema = SchemaBuilder.array().items(uuidSchema)
         it["a"] shouldBe GenericData.Array(schema, listOf(Utf8(uuid1.toString()), Utf8(uuid2.toString())))
      }
   }

   "read / write nullable UUIDs" {

      @Serializable
      data class UUIDTest(val a: UUID?)

      val uuid = UUID.randomUUID()

      writeRead(UUIDTest(uuid), UUIDTest.serializer()) {
         it["a"] shouldBe Utf8(uuid.toString())
      }

      writeRead(UUIDTest(null), UUIDTest.serializer()) {
         it["a"] shouldBe null
      }
   }
})