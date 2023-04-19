@file:UseContextualSerialization(forClasses = [UUID::class])

package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseContextualSerialization
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.serializer
import org.apache.avro.Schema.Parser
import java.util.*

@Serializable
@JvmInline
value class BooleanWrapper(val value: Boolean)
@Serializable
@JvmInline
value class ByteWrapper(val value: Byte)
@Serializable
@JvmInline
value class ShortWrapper(val value: Short)
@Serializable
@JvmInline
value class IntWrapper(val value: Int)
@Serializable
@JvmInline
value class LongWrapper(val value: Long)
@Serializable
@JvmInline
value class FloatWrapper(val value: Float)
@Serializable
@JvmInline
value class DoubleWrapper(val value: Double)
@Serializable
@JvmInline
value class ByteArrayWrapper(val value: ByteArray)
@Serializable
@JvmInline
value class StringWrapper(val value: String)

@Serializable
@JvmInline
value class UuidWrapper(
   @Contextual
   val value: UUID
)
class PrimitiveSchemaTest : StringSpec({

   "boolean value class should be boolean primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_boolean.json"))
      val schema = Avro.default.schema(BooleanWrapper.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "boolean type should be boolean primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_boolean.json"))
      val schema = Avro.default.schema(Boolean.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "byte value class should be byte primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_int.json"))
      val schema = Avro.default.schema(ByteWrapper.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "byte type should be byte primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_int.json"))
      val schema = Avro.default.schema(Byte.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "short value class should be short primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_int.json"))
      val schema = Avro.default.schema(ShortWrapper.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "short type should be short primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_int.json"))
      val schema = Avro.default.schema(Short.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "int value class should be int primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_int.json"))
      val schema = Avro.default.schema(IntWrapper.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "int type should be int primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_int.json"))
      val schema = Avro.default.schema(Int.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "long value class should be long primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_long.json"))
      val schema = Avro.default.schema(LongWrapper.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "long type should be long primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_long.json"))
      val schema = Avro.default.schema(Long.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "float value class should be float primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_float.json"))
      val schema = Avro.default.schema(FloatWrapper.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "float type should be float primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_float.json"))
      val schema = Avro.default.schema(Float.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "double value class should be double primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_double.json"))
      val schema = Avro.default.schema(DoubleWrapper.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "double type should be double primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_double.json"))
      val schema = Avro.default.schema(Double.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "bytes value class should be bytes primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_bytes.json"))
      val schema = Avro.default.schema(ByteArrayWrapper.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "bytes type should be bytes primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_bytes.json"))
      val schema = Avro.default.schema(ByteArraySerializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "string value class should be string primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_string.json"))
      val schema = Avro.default.schema(StringWrapper.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "string type should be string primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_string.json"))
      val schema = Avro.default.schema(String.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   "uuid value class should be string primitive schema" {
      val expected = Parser().parse(javaClass.getResourceAsStream("/primitive_string.json"))
      val schema = Avro.default.schema(UuidWrapper.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }   
})