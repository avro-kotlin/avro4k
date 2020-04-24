package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroDefault
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable

class AvroDefaultSchemaTest : FunSpec() {
   init {
      test("schema for data class with @AvroDefault should include default value as a string") {
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_default_annotation_string.json"))
         val schema = Avro.default.schema(BarString.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }

      test("schema for data class with @AvroDefault should include default value as an int") {
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_default_annotation_int.json"))
         val schema = Avro.default.schema(BarInt.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }

      test("schema for data class with @AvroDefault should include default value as a float") {
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_default_annotation_float.json"))
         val schema = Avro.default.schema(BarFloat.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }

      test("schema for data class with @AvroDefault should include default value as an array") {
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_default_annotation_array.json"))
         val schema = Avro.default.schema(BarArray.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }

      test("schema for data class with @AvroDefault should throw error when array type does not match default value type") {
         shouldThrow<NumberFormatException> { Avro.default.schema(BarInvalidArrayType.serializer()) }
         shouldThrow<IllegalArgumentException> { Avro.default.toRecord(BarInvalidNonPrimitiveType.serializer(), BarInvalidNonPrimitiveType()) }
      }
   }
}


@Serializable
data class BarString(
   val a: String,
   @AvroDefault("hello")
   val b: String,
   @AvroDefault(Avro.NULL)
   val nullableString: String?,
   @AvroDefault("hello")
   val c: String?
)

@Serializable
data class BarInt(
   val a: String,
   @AvroDefault("5")
   val b: Int,
   @AvroDefault(Avro.NULL)
   val nullableInt: Int?,
   @AvroDefault("5")
   val c: Int?
)

@Serializable
data class BarFloat(
   val a: String,
   @AvroDefault("3.14")
   val b: Float,
   @AvroDefault(Avro.NULL)
   val nullableFloat: Float?,
   @AvroDefault("3.14")
   val c: Float?
)

@Serializable
data class BarArray(
   @AvroDefault(Avro.EMPTY_LIST)
   val defaultEmptyArray: List<String>,
   @AvroDefault(Avro.NULL)
   val nullableDefaultEmptyArray: List<String>?,
   @AvroDefault("1,2")
   val defaultStringArrayWith2Defaults: List<String>,
   @AvroDefault("1,2")
   val defaultIntArrayWith2Defaults: List<Int>,
   @AvroDefault("3.14, 9.89")
   val defaultFloatArrayWith2Defaults: List<Float>
)

@Serializable
data class BarInvalidArrayType(
   @AvroDefault("foo-bar")
   val defaultFloatArrayWith2Defaults: List<Float>
)

@Serializable
class FooBar

@Serializable
data class BarInvalidNonPrimitiveType(
   @AvroDefault("test-value")
   val defaultBarArrayWithNonWorkingDefaults: List<FooBar> = ArrayList()
)



