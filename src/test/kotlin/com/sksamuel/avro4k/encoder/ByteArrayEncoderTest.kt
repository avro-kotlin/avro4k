package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
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
      val record = GenericData.Record(schema)
      record.put("z", ByteBuffer.wrap(byteArrayOf(1, 4, 9)))
      Avro.default.toRecord(ByteArrayTest.serializer(), ByteArrayTest(byteArrayOf(1, 4, 9))) shouldBe
         ListRecord(schema, ByteBuffer.allocate(3).put(1).put(4).put(9))
   }

   test("encode List<Byte> to ByteBuffer") {
      val schema = Avro.default.schema(ListByteTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", ByteBuffer.wrap(byteArrayOf(1, 4, 9)))
      Avro.default.toRecord(ListByteTest.serializer(), ListByteTest(listOf(1, 4, 9))) shouldBe
         ListRecord(schema, ByteBuffer.allocate(3).put(1).put(4).put(9))
   }

   test("encode Array<Byte> to ByteBuffer") {
      val schema = Avro.default.schema(ArrayByteTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", ByteBuffer.wrap(byteArrayOf(1, 4, 9)))
      Avro.default.toRecord(ArrayByteTest.serializer(), ArrayByteTest(arrayOf(1, 4, 9))) shouldBe
         ListRecord(schema, ByteBuffer.allocate(3).put(1).put(4).put(9))
   }
})