package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class BasicEncoderTest : WordSpec({

   "Encoder" should {
      "encode strings as UTF8"  {

         val schema = Avro.default.schema(StringFoo.serializer())
         val record = Avro.default.toRecord(StringFoo.serializer(), StringFoo("hello"))
         record shouldBe ListRecord(schema, Utf8("hello"))
      }
      "encode strings as GenericFixed and pad bytes when schema is Type.FIXED" {

         val schema = SchemaBuilder.record("Foo").fields()
            .name("s").type(Schema.createFixed("FixedString", null, null, 7)).noDefault()
            .endRecord()

         (Avro.default.toRecord(StringFoo.serializer(), schema, StringFoo("hello"))["s"] as GenericData.Fixed).bytes() shouldBe
            byteArrayOf(104, 101, 108, 108, 111, 0, 0)
      }
      "encode longs" {

         val schema = Avro.default.schema(LongFoo.serializer())
         val record = Avro.default.toRecord(LongFoo.serializer(), LongFoo(123456L))
         record shouldBe ListRecord(schema, 123456L)
      }
      "encode doubles" {

         val schema = Avro.default.schema(DoubleFoo.serializer())
         val record = Avro.default.toRecord(DoubleFoo.serializer(), DoubleFoo(123.435))
         record shouldBe ListRecord(schema, 123.435)
      }
      "encode booleans" {

         val schema = Avro.default.schema(BooleanFoo.serializer())
         val record = Avro.default.toRecord(BooleanFoo.serializer(), BooleanFoo(true))
         record shouldBe ListRecord(schema, true)
      }
      "encode floats" {

         val schema = Avro.default.schema(FloatFoo.serializer())
         val record = Avro.default.toRecord(FloatFoo.serializer(), FloatFoo(123.435F))
         record shouldBe ListRecord(schema, 123.435F)
      }
      "encode ints" {

         val schema = Avro.default.schema(IntFoo.serializer())
         val record = Avro.default.toRecord(IntFoo.serializer(), IntFoo(123))
         record shouldBe ListRecord(schema, 123)
      }
      "encode shorts" {

         val schema = Avro.default.schema(ShortFoo.serializer())
         val record = Avro.default.toRecord(ShortFoo.serializer(), ShortFoo(123.toShort()))
         record shouldBe ListRecord(schema, 123.toShort())
      }
      "encode bytes" {

         val schema = Avro.default.schema(ByteFoo.serializer())
         val record = Avro.default.toRecord(ByteFoo.serializer(), ByteFoo(123.toByte()))
         record shouldBe ListRecord(schema, 123.toByte())
      }
   }

}) {
   @Serializable
   data class StringFoo(val s: String)

   @Serializable
   data class LongFoo(val l: Long)

   @Serializable
   data class DoubleFoo(val d: Double)

   @Serializable
   data class BooleanFoo(val d: Boolean)

   @Serializable
   data class FloatFoo(val d: Float)

   @Serializable
   data class IntFoo(val i: Int)

   @Serializable
   data class ShortFoo(val s: Short)

   @Serializable
   data class ByteFoo(val b: Byte)
}
