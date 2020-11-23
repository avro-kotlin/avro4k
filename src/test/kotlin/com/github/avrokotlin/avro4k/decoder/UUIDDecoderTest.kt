@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.util.*

class UUIDDecoderTest : StringSpec({

   "decode uuids"  {

      @Serializable
      data class UUIDTest(val uuid: UUID)

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDTest.serializer())
      val record = GenericData.Record(schema)
      record.put("uuid", uuid.toString())

      Avro.default.fromRecord(UUIDTest.serializer(), record) shouldBe UUIDTest(uuid)
   }

   "decode UUIDSs encoded as Utf8"  {

      @Serializable
      data class UUIDTest(val uuid: UUID)

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDTest.serializer())

      val record = GenericData.Record(schema)
      record.put("uuid", Utf8(uuid.toString()))

      Avro.default.fromRecord(UUIDTest.serializer(), record) shouldBe UUIDTest(uuid)
   }

   "decode list of uuids"  {

      @Serializable
      data class UUIDTest(val uuids: List<UUID>)

      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDTest.serializer())

      val record = GenericData.Record(schema)
      record.put("uuids", listOf(uuid1.toString(), uuid2.toString()))

      Avro.default.fromRecord(UUIDTest.serializer(), record) shouldBe UUIDTest(listOf(uuid1, uuid2))
   }

   "decode nullable UUIDs"  {

      @Serializable
      data class UUIDTest(val uuid: UUID?)

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDTest.serializer())

      val record1 = GenericData.Record(schema)
      record1.put("uuid", uuid.toString())

      Avro.default.fromRecord(UUIDTest.serializer(), record1) shouldBe UUIDTest(uuid)

      val record2 = GenericData.Record(schema)
      record2.put("uuid", null)

      Avro.default.fromRecord(UUIDTest.serializer(), record2) shouldBe UUIDTest(null)
   }
})