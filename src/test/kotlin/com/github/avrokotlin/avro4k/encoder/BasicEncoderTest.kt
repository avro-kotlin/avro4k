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

         @Serializable
         data class Foo(val s: String)

         val schema = Avro.default.schema(Foo.serializer())
         val record = Avro.default.toRecord(Foo.serializer(), Foo("hello"))
         record shouldBe ListRecord(schema, Utf8("hello"))
      }
      "encode strings as GenericFixed and pad bytes when schema is Type.FIXED" {
         @Serializable
         data class Foo(val s: String)

         val schema = SchemaBuilder.record("Foo").fields()
            .name("s").type(Schema.createFixed("FixedString", null, null, 7)).noDefault()
            .endRecord()

         (Avro.default.toRecord(Foo.serializer(), schema, Foo("hello"))["s"] as GenericData.Fixed).bytes() shouldBe
            byteArrayOf(104, 101, 108, 108, 111, 0, 0)
      }
      "encode longs" {
         @Serializable
         data class Foo(val l: Long)

         val schema = Avro.default.schema(Foo.serializer())
         val record = Avro.default.toRecord(Foo.serializer(), Foo(123456L))
         record shouldBe ListRecord(schema, 123456L)
      }
      "encode doubles" {
         @Serializable
         data class Foo(val d: Double)

         val schema = Avro.default.schema(Foo.serializer())
         val record = Avro.default.toRecord(Foo.serializer(), Foo(123.435))
         record shouldBe ListRecord(schema, 123.435)
      }
      "encode booleans" {
         @Serializable
         data class Foo(val d: Boolean)

         val schema = Avro.default.schema(Foo.serializer())
         val record = Avro.default.toRecord(Foo.serializer(), Foo(true))
         record shouldBe ListRecord(schema, true)
      }
      "encode floats" {
         @Serializable
         data class Foo(val d: Float)

         val schema = Avro.default.schema(Foo.serializer())
         val record = Avro.default.toRecord(Foo.serializer(), Foo(123.435F))
         record shouldBe ListRecord(schema, 123.435F)
      }
      "encode ints" {
         @Serializable
         data class Foo(val i: Int)

         val schema = Avro.default.schema(Foo.serializer())
         val record = Avro.default.toRecord(Foo.serializer(), Foo(123))
         record shouldBe ListRecord(schema, 123)
      }
      "encode shorts" {
         @Serializable
         data class Foo(val s: Short)

         val schema = Avro.default.schema(Foo.serializer())
         val record = Avro.default.toRecord(Foo.serializer(), Foo(123.toShort()))
         record shouldBe ListRecord(schema, 123.toShort())
      }
      "encode bytes" {
         @Serializable
         data class Foo(val b: Byte)

         val schema = Avro.default.schema(Foo.serializer())
         val record = Avro.default.toRecord(Foo.serializer(), Foo(123.toByte()))
         record shouldBe ListRecord(schema, 123.toByte())
      }
   }

})

