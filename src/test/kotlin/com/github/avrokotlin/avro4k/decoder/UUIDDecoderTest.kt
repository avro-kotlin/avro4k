@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.util.*

class UUIDDecoderTest : StringSpec({

   "decode UUIDs encoded as Utf8"  {
      @Serializable
      data class UUIDTest(val uuid: UUID)

      val uuid = UUID.randomUUID()
      val schema = Avro.default.schema(UUIDTest.serializer())

      val record = GenericData.Record(schema)
      record.put("uuid", Utf8(uuid.toString()))

      Avro.default.fromRecord(UUIDTest.serializer(), record) shouldBe UUIDTest(uuid)
   }
})
