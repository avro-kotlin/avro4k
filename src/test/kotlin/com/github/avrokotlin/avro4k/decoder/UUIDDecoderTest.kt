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

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDTest.serializer())
      val record = GenericData.Record(schema)
      record.put("uuid", uuid.toString())

      Avro.default.fromRecord(UUIDTest.serializer(), record) shouldBe UUIDTest(uuid)
   }

   "decode UUIDSs encoded as Utf8"  {

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDTest.serializer())

      val record = GenericData.Record(schema)
      record.put("uuid", Utf8(uuid.toString()))

      Avro.default.fromRecord(UUIDTest.serializer(), record) shouldBe UUIDTest(uuid)
   }

   "decode list of uuids"  {

      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDListTest.serializer())

      val record = GenericData.Record(schema)
      record.put("uuids", listOf(uuid1.toString(), uuid2.toString()))

      Avro.default.fromRecord(UUIDListTest.serializer(), record) shouldBe UUIDListTest(listOf(uuid1, uuid2))
   }

   "decode nullable UUIDs"  {

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDNullableTest.serializer())

      val record1 = GenericData.Record(schema)
      record1.put("uuid", uuid.toString())

      Avro.default.fromRecord(UUIDNullableTest.serializer(), record1) shouldBe UUIDNullableTest(uuid)

      val record2 = GenericData.Record(schema)
      record2.put("uuid", null)

      Avro.default.fromRecord(UUIDNullableTest.serializer(), record2) shouldBe UUIDNullableTest(null)
   }
}) {
   @Serializable
   data class UUIDTest(val uuid: UUID)

   @Serializable
   data class UUIDListTest(val uuids: List<UUID>)


   @Serializable
   data class UUIDNullableTest(val uuid: UUID?)
}
