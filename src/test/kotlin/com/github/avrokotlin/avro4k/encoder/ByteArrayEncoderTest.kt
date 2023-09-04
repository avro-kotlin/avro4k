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
import java.nio.ByteBuffer

class ByteArrayEncoderTest : FunSpec({

   test("encode ByteArray to ByteBuffer to ") {
      val schema = Avro.default.schema(ByteArrayTest.serializer())
      Avro.default.encodeToGenericData(ByteArrayTest(byteArrayOf(1, 4, 9))) shouldBeContentOf
              ListRecord(schema, ByteBuffer.wrap(byteArrayOf(1, 4, 9)))
   }

   test("encode List<Byte> to ByteBuffer") {
      val schema = Avro.default.schema(ListByteTest.serializer())
      Avro.default.encodeToGenericData(ListByteTest(listOf(1, 4, 9))) shouldBeContentOf
              ListRecord(schema, ByteBuffer.wrap(byteArrayOf(1, 4, 9)))
   }

   test("encode Array<Byte> to ByteBuffer") {
      val schema = Avro.default.schema(ArrayByteTest.serializer())
      Avro.default.encodeToGenericData(ArrayByteTest(arrayOf(1, 4, 9))) shouldBeContentOf
              ListRecord(schema, ByteBuffer.wrap(byteArrayOf(1, 4, 9)))
   }

   test("encode ByteArray as FIXED when schema is Type.Fixed") {
       val schema = SchemaBuilder.record("ByteArrayTest").fields()
               .name("z").type(Schema.createFixed("ByteArray", null, null, 8)).noDefault()
               .endRecord()
       val record = Avro.default.encodeToGenericData(schema, ByteArrayTest(byteArrayOf(1, 4, 9)))
       record shouldBeContentOf ListRecord(schema, GenericData.get().createFixed(null, byteArrayOf(0, 0, 0, 0, 0, 1, 4, 9), schema.fields[0].schema()))
   }
}) {

   @Serializable
   data class ByteArrayTest(val z: ByteArray)

   @Serializable
   data class ArrayByteTest(val z: Array<Byte>)

   @Serializable
   data class ListByteTest(val z: List<Byte>)
}
