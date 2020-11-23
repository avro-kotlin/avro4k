package com.github.avrokotlin.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroDefault
import com.sksamuel.avro4k.serializer.BigDecimalSerializer
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.AvroTypeException
import java.math.BigDecimal

@Suppress("BlockingMethodInNonBlockingContext")
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

      test("schema for data class with @AvroDefault should include default value as a BigDecimal") {
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_default_annotation_big_decimal.json"))
         val schema = Avro.default.schema(BarDecimal.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }

      test("schema for data class with @AvroDefault should include default value as a list") {
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_default_annotation_list.json"))
         val schema = Avro.default.schema(BarList.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }

      test("schema for data class with @AvroDefault should include default value as a list with a record element type") {
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_default_annotation_list_of_records.json"))
         val schema = Avro.default.schema(BarListOfElements.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }

      test("schema for data class with @AvroDefault should include default value as an array") {
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_default_annotation_array.json"))
         val schema = Avro.default.schema(BarArray.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }

      test("schema for data class with @AvroDefault should include default value as an set") {
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/avro_default_annotation_set.json"))
         val schema = Avro.default.schema(BarSet.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }

      test("schema for data class with @AvroDefault should throw error when array type does not match default value type") {
         shouldThrow<AvroTypeException> { Avro.default.schema(BarInvalidArrayType.serializer()) }
         shouldThrow<AvroTypeException> { Avro.default.toRecord(BarInvalidNonArrayType.serializer(), BarInvalidNonArrayType()) }
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
data class BarDecimal(
   @Serializable(BigDecimalSerializer::class)
   val a: BigDecimal,
   @Serializable(BigDecimalSerializer::class)
   @AvroDefault("\u0000")
   val b: BigDecimal,
   @Serializable(BigDecimalSerializer::class)
   @AvroDefault(Avro.NULL)
   val nullableString: BigDecimal?,
   @Serializable(BigDecimalSerializer::class)
   @AvroDefault("\u0000")
   val c: BigDecimal?
)

@Serializable
data class BarSet(
   @AvroDefault("[]")
   val defaultEmptySet: Set<String>,
   @AvroDefault(Avro.NULL)
   val nullableDefaultEmptySet: Set<String>?,
   @AvroDefault("""["John", "Doe"]""")
   val defaultStringSetWith2Defaults: Set<String>,
   @AvroDefault("""[1, 2]""")
   val defaultIntSetWith2Defaults: Set<Int>,
   @AvroDefault("""[3.14, 9.89]""")
   val defaultFloatSetWith2Defaults: Set<Float>,
   //Unions are currently not correctly supported by Java-Avro, so for now we do not test with null values in
   //the default
   //See https://issues.apache.org/jira/browse/AVRO-2647
   @AvroDefault("""[null]""")
   val defaultStringSetWithNullableTypes : Set<String?>
)
@Serializable
data class BarList(
   @AvroDefault("[]")
   val defaultEmptyList: List<String>,
   @AvroDefault(Avro.NULL)
   val nullableDefaultEmptyList: List<String>?,
   @AvroDefault("""["John", "Doe"]""")
   val defaultStringListWith2Defaults: List<String>,
   @AvroDefault("""[1, 2]""")
   val defaultIntListWith2Defaults: List<Int>,
   @AvroDefault("""[3.14, 9.89]""")
   val defaultFloatListWith2Defaults: List<Float>,
   //Unions are currently not correctly supported by Java-Avro, so for now we do not test with null values in
   //the default
   //See https://issues.apache.org/jira/browse/AVRO-2647
   @AvroDefault("""[null]""")
   val defaultStringListWithNullableTypes : List<String?>
)

@Serializable
data class FooElement(val value: String)

@Serializable
data class BarListOfElements(
   @AvroDefault("[]")
   val defaultEmptyListOfRecords: List<FooElement>,
   @AvroDefault("""[{"value":"foo"}]""")
   val defaultListWithOneValue : List<FooElement>
)

@Suppress("ArrayInDataClass")
@Serializable
data class BarArray(
   @AvroDefault("[]")
   val defaultEmptyArray: Array<String>,
   @AvroDefault(Avro.NULL)
   val nullableDefaultEmptyArray: Array<String>?,
   @AvroDefault("""["John", "Doe"]""")
   val defaultStringArrayWith2Defaults: Array<String>,
   @AvroDefault("""[1, 2]""")
   val defaultIntArrayWith2Defaults: Array<Int>,
   @AvroDefault("""[3.14, 9.89]""")
   val defaultFloatArrayWith2Defaults: Array<Float>,
   //Unions are currently not correctly supported by Java-Avro, so for now we do not test with null values in
   //the default
   //See https://issues.apache.org/jira/browse/AVRO-2647
   @AvroDefault("""[null]""")
   val defaultStringArrayWithNullableTypes : Array<String?>
)

@Serializable
data class BarInvalidArrayType(
   @com.sksamuel.avro4k.AvroDefault("""["foo-bar"]""")
   val defaultFloatArrayWith2Defaults: List<Float>
)

@Serializable
data class BarInvalidNonArrayType(
   @AvroDefault("{}")
   val defaultBarArrayWithNonWorkingDefaults: List<Int> = ArrayList()
)



