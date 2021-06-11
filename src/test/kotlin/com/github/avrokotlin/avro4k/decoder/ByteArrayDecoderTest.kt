package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

class ByteArrayDecoderTest : FunSpec({

   test("decode ByteBuffer to ByteArray") {
      val schema = Avro.default.schema(ByteArrayTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", ByteBuffer.wrap(byteArrayOf(1, 4, 9)))
      Avro.default.fromRecord(ByteArrayTest.serializer(), record).z shouldBe byteArrayOf(1, 4, 9)
   }

   test("decode ByteBuffer to List<Byte>") {
      val schema = Avro.default.schema(ListByteTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", ByteBuffer.wrap(byteArrayOf(1, 4, 9)))
      Avro.default.fromRecord(ListByteTest.serializer(), record).z shouldBe listOf<Byte>(1, 4, 9)
   }

   test("decode ByteBuffer to Array<Byte>") {
      val schema = Avro.default.schema(ArrayByteTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", ByteBuffer.wrap(byteArrayOf(1, 4, 9)))
      Avro.default.fromRecord(ArrayByteTest.serializer(), record).z shouldBe arrayOf<Byte>(1, 4, 9)
   }

   test("decode ByteArray to ByteArray") {
      val schema = Avro.default.schema(ByteArrayTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", byteArrayOf(1, 4, 9))
      Avro.default.fromRecord(ByteArrayTest.serializer(), record).z shouldBe byteArrayOf(1, 4, 9)
   }

   test("decode ByteArray to List<Byte>") {
      val schema = Avro.default.schema(ListByteTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", byteArrayOf(1, 4, 9))
      Avro.default.fromRecord(ListByteTest.serializer(), record).z shouldBe listOf<Byte>(1, 4, 9)
   }

   test("decode ByteArray to Array<Byte>") {
      val schema = Avro.default.schema(ArrayByteTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", byteArrayOf(1, 4, 9))
      Avro.default.fromRecord(ArrayByteTest.serializer(), record).z shouldBe arrayOf<Byte>(1, 4, 9)
   }

   test("decode Array<Byte> to ByteArray") {
      val schema = Avro.default.schema(ByteArrayTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", arrayOf<Byte>(1, 4, 9))
      Avro.default.fromRecord(ByteArrayTest.serializer(), record).z shouldBe byteArrayOf(1, 4, 9)
   }

   test("decode Array<Byte> to List<Byte>") {
      val schema = Avro.default.schema(ListByteTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", arrayOf<Byte>(1, 4, 9))
      Avro.default.fromRecord(ListByteTest.serializer(), record).z shouldBe listOf<Byte>(1, 4, 9)
   }

   test("decode Array<Byte> to Array<Byte>") {
      val schema = Avro.default.schema(ArrayByteTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", arrayOf<Byte>(1, 4, 9))
      Avro.default.fromRecord(ArrayByteTest.serializer(), record).z shouldBe arrayOf<Byte>(1, 4, 9)
   }

   test("decode List<Byte> to ByteArray") {
      val schema = Avro.default.schema(ByteArrayTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", listOf<Byte>(1, 4, 9))
      Avro.default.fromRecord(ByteArrayTest.serializer(), record).z shouldBe byteArrayOf(1, 4, 9)
   }

   test("decode List<Byte> to List<Byte>") {
      val schema = Avro.default.schema(ListByteTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", listOf<Byte>(1, 4, 9))
      Avro.default.fromRecord(ListByteTest.serializer(), record).z shouldBe listOf<Byte>(1, 4, 9)
   }

   test("decode List<Byte> to Array<Byte>") {
      val schema = Avro.default.schema(ArrayByteTest.serializer())
      val record = GenericData.Record(schema)
      record.put("z", listOf<Byte>(1, 4, 9))
      Avro.default.fromRecord(ArrayByteTest.serializer(), record).z shouldBe arrayOf<Byte>(1, 4, 9)
   }
}) {

   @Serializable
   data class ByteArrayTest(val z: ByteArray)

   @Serializable
   data class ArrayByteTest(val z: Array<Byte>)

   @Serializable
   data class ListByteTest(val z: List<Byte>)
}
