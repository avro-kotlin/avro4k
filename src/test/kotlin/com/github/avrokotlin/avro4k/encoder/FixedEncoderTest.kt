package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

class FixedEncoderTest : FunSpec({

   test("encode strings as GenericFixed when schema is Type.FIXED") {

      val schema = SchemaBuilder.record("foo").fields()
              .name("s").type(Schema.createFixed("s", null, null, 5)).noDefault()
              .endRecord()

      val record = Avro.default.encodeToGenericData(StringFoo("hello"), schema)
      record shouldBeContentOf ListRecord(schema, GenericData.get().createFixed(
              null,
              byteArrayOf(104, 101, 108, 108, 111),
              Schema.createFixed("s", null, null, 5)
      ))
   }

   test("encode byte arrays as GenericFixed when schema is Type.FIXED") {

      val schema = SchemaBuilder.record("foo").fields()
              .name("s").type(Schema.createFixed("s", null, null, 5)).noDefault()
              .endRecord()

      val record = Avro.default.encodeToGenericData(ByteArrayFoo(byteArrayOf(1, 2, 3, 4, 5)), schema)
      record shouldBeContentOf ListRecord(schema, GenericData.get().createFixed(
              null,
              byteArrayOf(1, 2, 3, 4, 5),
              Schema.createFixed("s", null, null, 5)
      )
)   }
}) {
   @Serializable
   data class StringFoo(val s: String)

   @Serializable
   data class ByteArrayFoo(val s: ByteArray)
}
