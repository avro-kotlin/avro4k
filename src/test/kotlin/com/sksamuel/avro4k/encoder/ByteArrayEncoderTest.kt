package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

class ByteArrayEncoderTest : FunSpec({

   @Serializable
   data class ByteArrayTest(val z: ByteArray)

   @Serializable
   data class ArrayByteTest(val z: Array<Byte>)

   @Serializable
   data class ListByteTest(val z: List<Byte>)

   test("encode ByteArray to ByteBuffer to ") {
      val schema = Avro.default.schema(ByteArrayTest.serializer())
      Avro.default.toRecord(ByteArrayTest.serializer(), ByteArrayTest(byteArrayOf(1, 4, 9))) shouldBe
         ListRecord(schema, ByteBuffer.allocate(3).put(1).put(4).put(9))
   }

   test("encode List<Byte> to ByteBuffer") {
      val schema = Avro.default.schema(ListByteTest.serializer())
      Avro.default.toRecord(ListByteTest.serializer(), ListByteTest(listOf(1, 4, 9))) shouldBe
         ListRecord(schema, ByteBuffer.allocate(3).put(1).put(4).put(9))
   }

   test("encode Array<Byte> to ByteBuffer") {
      val schema = Avro.default.schema(ArrayByteTest.serializer())
      Avro.default.toRecord(ArrayByteTest.serializer(), ArrayByteTest(arrayOf(1, 4, 9))) shouldBe
         ListRecord(schema, ByteBuffer.allocate(3).put(1).put(4).put(9))
   }

   test("encode ByteArray as FIXED when schema is Type.Fixed") {
      val schema = SchemaBuilder.record("ByteArrayTest").fields()
         .name("z").type(Schema.createFixed("ByteArray", null, null, 8)).noDefault()
         .endRecord()
      val record = Avro.default.toRecord(ByteArrayTest.serializer(), schema, ByteArrayTest(byteArrayOf(1, 4, 9)))
      val fixed = record.get("z") as GenericFixed
      fixed.bytes() shouldBe byteArrayOf(0, 0, 0, 0, 0, 1, 4, 9)
   }
})