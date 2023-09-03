@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.util.Utf8
import java.util.UUID

class UUIDEncoderTest : FunSpec({

   test("encode uuids") {

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDTest.serializer())
      Avro.default.encode(UUIDTest.serializer(), UUIDTest(uuid)) shouldBeContentOf
              ListRecord(schema, Utf8(uuid.toString()))
   }

   test("encode lists of uuids") {

      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDList.serializer())
      val actual = Avro.default.encode(UUIDList.serializer(), UUIDList(listOf(uuid1, uuid2)))
      val expected = ListRecord(schema, listOf(listOf(Utf8(uuid1.toString()), Utf8(uuid2.toString()))))
      actual shouldBeContentOf expected
   }

   test("encode nullable uuids") {

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(NullableUUIDTest.serializer())
      Avro.default.encode(NullableUUIDTest.serializer(), NullableUUIDTest(uuid)) shouldBeContentOf ListRecord(schema, Utf8(uuid.toString()))
      Avro.default.encode(NullableUUIDTest.serializer(), NullableUUIDTest(null)) shouldBeContentOf ListRecord(schema, null)
   }
}) {
   @Serializable
   data class UUIDTest(val uuid: UUID)

   @Serializable
   data class UUIDList(val uuids: List<UUID>)

   @Serializable
   data class NullableUUIDTest(val uuid: UUID?)
}
