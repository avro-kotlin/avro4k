package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroDefault
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable

class AvroDefaultSchemaTest: FunSpec() {
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
   }
}


@Serializable
data class BarString(
   val a: String,
   @AvroDefault("hello")
   val b: String,
   @AvroDefault(Avro.NULL)
   val nullableString : String?,
   @AvroDefault("hello")
   val c:String?
)

@Serializable
data class BarInt(
   val a: String,
   @AvroDefault("5")
   val b: Int,
   @AvroDefault(Avro.NULL)
   val nullableInt : Int?,
   @AvroDefault("5")
   val c:Int?
)

@Serializable
data class BarFloat(
   val a: String,
   @AvroDefault("3.14")
   val b: Float,
   @AvroDefault(Avro.NULL)
   val nullableFloat : Float?,
   @AvroDefault("3.14")
   val c:Float?

)
