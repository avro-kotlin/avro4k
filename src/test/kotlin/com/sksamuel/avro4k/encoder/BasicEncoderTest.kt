package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.MapRecord
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
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
       record shouldBe MapRecord(schema, mapOf("s" to Utf8("hello")))
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
       record shouldBe MapRecord(schema, mapOf("l" to 123456L))
    }
    "encode doubles" {
      @Serializable
      data class Foo(val d: Double)

      val schema = Avro.default.schema(Foo.serializer())
      val record = Avro.default.toRecord(Foo.serializer(), Foo(123.435))
       record shouldBe MapRecord(schema, mapOf("d" to 123.435))
    }
    "encode booleans" {
      @Serializable
      data class Foo(val d: Boolean)

      val schema = Avro.default.schema(Foo.serializer())
      val record = Avro.default.toRecord(Foo.serializer(), Foo(true))
       record shouldBe MapRecord(schema, mapOf("d" to true))
    }
    "encode floats" {
      @Serializable
      data class Foo(val d: Float)

      val schema = Avro.default.schema(Foo.serializer())
      val record = Avro.default.toRecord(Foo.serializer(), Foo(123.435F))
       record shouldBe MapRecord(schema, mapOf("d" to 123.435F))
    }
    "encode ints" {
      @Serializable
      data class Foo(val i: Int)

      val schema = Avro.default.schema(Foo.serializer())
      val record = Avro.default.toRecord(Foo.serializer(), Foo(123))
       record shouldBe MapRecord(schema, mapOf("i" to 123))
    }
    "encode shorts" {
      @Serializable
      data class Foo(val s: Short)

      val schema = Avro.default.schema(Foo.serializer())
      val record = Avro.default.toRecord(Foo.serializer(), Foo(123.toShort()))
       record shouldBe MapRecord(schema, mapOf("s" to 123.toShort()))
    }
    "encode bytes" {
      @Serializable
      data class Foo(val b: Byte)

      val schema = Avro.default.schema(Foo.serializer())
      val record = Avro.default.toRecord(Foo.serializer(), Foo(123.toByte()))
       record shouldBe MapRecord(schema, mapOf("b" to 123.toByte()))
    }
  }

})

