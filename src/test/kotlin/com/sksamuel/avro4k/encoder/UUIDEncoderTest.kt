@file:UseSerializers(UUIDSerializer::class)

package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import com.sksamuel.avro4k.serializers.UUIDSerializer
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.util.Utf8
import java.util.*

class UUIDEncoderTest : FunSpec({

   test("encode uuids") {

      @Serializable
      data class UUIDTest(val uuid: UUID)

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDTest.serializer())
      Avro.default.toRecord(UUIDTest.serializer(), UUIDTest(uuid)) shouldBe
         ListRecord(schema, Utf8(uuid.toString()))
   }

   test("encode lists of uuids") {

      @Serializable
      data class UUIDList(val uuids: List<UUID>)

      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDList.serializer())
      val actual = Avro.default.toRecord(UUIDList.serializer(), UUIDList(listOf(uuid1, uuid2)))
      val expected = ListRecord(schema, listOf(listOf(Utf8(uuid1.toString()), Utf8(uuid2.toString()))))
      actual shouldBe expected
   }

   test("encode nullable uuids") {

      @Serializable
      data class UUIDTest(val uuid: UUID?)

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDTest.serializer())
      Avro.default.toRecord(UUIDTest.serializer(), UUIDTest(uuid)) shouldBe ListRecord(schema, Utf8(uuid.toString()))
      Avro.default.toRecord(UUIDTest.serializer(), UUIDTest(null)) shouldBe ListRecord(schema, null)
   }
})