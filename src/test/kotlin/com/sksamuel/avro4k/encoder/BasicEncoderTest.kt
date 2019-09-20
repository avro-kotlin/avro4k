package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.DefaultNamingStrategy
import com.sksamuel.avro4k.ImmutableRecord
import com.sksamuel.avro4k.encoderFor
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class BasicEncoderTest : WordSpec({

  "Encoder" should {
    "encode strings as UTF8"  {

      @Serializable
      data class Foo(val s: String)

      val schema = Avro.default.schema(Foo.serializer())
      val record = Avro.default.toRecord(Foo.serializer(), Foo("hello"))
      record shouldBe ImmutableRecord(schema, arrayListOf(Utf8("hello")))
    }
//    "encode strings as GenericFixed and pad bytes when schema is fixed" {
//      data class Foo(val s: String)
//      implicit object StringFixedSchemaFor extends SchemaFor[String] {
//      override def schema(fieldMapper: FieldMapper): Schema = Schema.createFixed("FixedString", null, null, 7)
//    }
//      val schema = Avro.default.schema(Foo.serializer())
//      val record = encoderFor<Foo>().encode(Foo("hello"), schema, DefaultFieldMapper).asInstanceOf[GenericRecord]
//      record.get("s").asInstanceOf[GenericFixed].bytes().toList shouldBe Seq(104, 101, 108, 108, 111, 0, 0)
//      // the fixed should have the right size
//      record.get("s").asInstanceOf[GenericFixed].bytes().length shouldBe 7
//    }
    "encode longs" {
      @Serializable
      data class Foo(val l: Long)

      val schema = Avro.default.schema(Foo.serializer())
      val record = Avro.default.toRecord(Foo.serializer(), Foo(123456L))
      record shouldBe ImmutableRecord(schema, arrayListOf(123456L))
    }
//    "encode doubles" {
//      @Serializable
//      data class Foo(val d: Double)
//
//      val schema = Avro.default.schema(Foo.serializer())
//      encoderFor<Foo>().encode(Foo(123.435), schema, DefaultNamingStrategy) shouldBe
//          ImmutableRecord(schema, arrayOf(java.lang.Double.valueOf(123.435)))
//    }
//    "encode booleans" {
//      @Serializable
//      data class Foo(val d: Boolean)
//
//      val schema = Avro.default.schema(Foo.serializer())
//      encoderFor<Foo>().encode(Foo(true), schema, DefaultNamingStrategy) shouldBe
//          ImmutableRecord(schema, arrayOf(java.lang.Boolean.valueOf(true)))
//    }
//    "encode floats" {
//      @Serializable
//      data class Foo(val d: Float)
//
//      val schema = Avro.default.schema(Foo.serializer())
//      encoderFor<Foo>().encode(Foo(123.435F), schema, DefaultNamingStrategy) shouldBe
//          ImmutableRecord(schema, arrayOf(java.lang.Float.valueOf(123.435F)))
//    }
//    "encode ints" {
//      @Serializable
//      data class Foo(val i: Int)
//
//      val schema = Avro.default.schema(Foo.serializer())
//      encoderFor<Foo>().encode(Foo(123), schema, DefaultNamingStrategy) shouldBe
//          ImmutableRecord(schema, arrayOf(Integer.valueOf(123)))
//    }
  }

})

