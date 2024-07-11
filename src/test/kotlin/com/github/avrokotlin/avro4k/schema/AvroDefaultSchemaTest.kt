package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.AvroTypeException
import java.math.BigDecimal
import kotlin.io.path.Path

internal class AvroDefaultSchemaTest : FunSpec() {
    init {
        test("schema for data class with @AvroDefault should include default value as a string") {
            AvroAssertions.assertThat<BarString>()
                .generatesSchema(Path("/avro_default_annotation_string.json"))
        }

        test("schema for data class with @AvroDefault should include default value as an int") {
            AvroAssertions.assertThat<BarInt>()
                .generatesSchema(Path("/avro_default_annotation_int.json"))
        }

        test("schema for data class with @AvroDefault should include default value as a float") {
            AvroAssertions.assertThat<BarFloat>()
                .generatesSchema(Path("/avro_default_annotation_float.json"))
        }

        test("schema for data class with @AvroDefault should include default value as a BigDecimal") {
            AvroAssertions.assertThat<BarDecimal>()
                .generatesSchema(Path("/avro_default_annotation_big_decimal.json"))
        }

        test("schema for data class with @AvroDefault should include default value as an Enum") {
            AvroAssertions.assertThat<BarEnum>()
                .generatesSchema(Path("/avro_default_annotation_enum.json"))
        }

        test("schema for data class with @AvroDefault should include default value as a list") {
            AvroAssertions.assertThat<BarList>()
                .generatesSchema(Path("/avro_default_annotation_list.json"))
        }

        test("schema for data class with @AvroDefault should include default value as a list with a record element type") {
            AvroAssertions.assertThat<BarListOfElements>()
                .generatesSchema(Path("/avro_default_annotation_list_of_records.json"))
        }

        test("schema for data class with @AvroDefault should include default value as an array") {
            AvroAssertions.assertThat<BarArray>()
                .generatesSchema(Path("/avro_default_annotation_array.json"))
        }

        test("schema for data class with @AvroDefault should include default value as an set") {
            AvroAssertions.assertThat<BarSet>()
                .generatesSchema(Path("/avro_default_annotation_set.json"))
        }

        test("schema for data class with @AvroDefault should throw error when array type does not match default value type") {
            shouldThrow<AvroTypeException> { Avro.schema(BarInvalidArrayType.serializer()) }
            shouldThrow<AvroTypeException> { Avro.schema(BarInvalidNonArrayType.serializer()) }
        }
    }

    @Serializable
    private data class BarString(
        val a: String,
        @AvroDefault("hello")
        val b: String,
        @AvroDefault("null")
        val nullableString: String?,
        @AvroDefault("hello")
        val c: String?,
    )

    @Serializable
    private data class BarInt(
        val a: String,
        @AvroDefault("5")
        val b: Int,
        @AvroDefault("null")
        val nullableInt: Int?,
        @AvroDefault("5")
        val c: Int?,
    )

    @Serializable
    private data class BarFloat(
        val a: String,
        @AvroDefault("3.14")
        val b: Float,
        @AvroDefault("null")
        val nullableFloat: Float?,
        @AvroDefault("3.14")
        val c: Float?,
    )

    private enum class FooEnum {
        A,
        B,
        C,
    }

    @Serializable
    private data class BarEnum(
        val a: FooEnum,
        @AvroDefault("A")
        val b: FooEnum,
        @AvroDefault("null")
        val nullableEnum: FooEnum?,
        @AvroDefault("B")
        val c: FooEnum?,
    )

    @Serializable
    private data class BarDecimal(
        @AvroDecimal(scale = 2, precision = 8)
        @Serializable(BigDecimalSerializer::class)
        val a: BigDecimal,
        @AvroDecimal(scale = 2, precision = 8)
        @Serializable(BigDecimalSerializer::class)
        @AvroDefault("\u0000")
        val b: BigDecimal,
        @AvroDecimal(scale = 2, precision = 8)
        @Serializable(BigDecimalSerializer::class)
        @AvroDefault("null")
        val nullableString: BigDecimal?,
        @AvroDecimal(scale = 2, precision = 8)
        @Serializable(BigDecimalSerializer::class)
        @AvroDefault("\u0000")
        val c: BigDecimal?,
    )

    @Serializable
    private data class BarSet(
        @AvroDefault("[]")
        val defaultEmptySet: Set<String>,
        @AvroDefault("null")
        val nullableDefaultEmptySet: Set<String>?,
        @AvroDefault("""["John", "Doe"]""")
        val defaultStringSetWith2Defaults: Set<String>,
        @AvroDefault("""[1, 2]""")
        val defaultIntSetWith2Defaults: Set<Int>,
        @AvroDefault("""[3.14, 9.89]""")
        val defaultFloatSetWith2Defaults: Set<Float>,
        // Unions are currently not correctly supported by Java-Avro, so for now we do not test with null values in
        // the default
        // See https://issues.apache.org/jira/browse/AVRO-2647
        @AvroDefault("""[null]""")
        val defaultStringSetWithNullableTypes: Set<String?>,
    )

    @Serializable
    private data class BarList(
        @AvroDefault("[]")
        val defaultEmptyList: List<String>,
        @AvroDefault("null")
        val nullableDefaultEmptyList: List<String>?,
        @AvroDefault("""["John", "Doe"]""")
        val defaultStringListWith2Defaults: List<String>,
        @AvroDefault("""[1, 2]""")
        val defaultIntListWith2Defaults: List<Int>,
        @AvroDefault("""[3.14, 9.89]""")
        val defaultFloatListWith2Defaults: List<Float>,
        // Unions are currently not correctly supported by Java-Avro, so for now we do not test with null values in
        // the default
        // See https://issues.apache.org/jira/browse/AVRO-2647
        @AvroDefault("""[null]""")
        val defaultStringListWithNullableTypes: List<String?>,
    )

    @Serializable
    private data class FooElement(val value: String)

    @Serializable
    private data class BarListOfElements(
        @AvroDefault("[]")
        val defaultEmptyListOfRecords: List<FooElement>,
        @AvroDefault("""[{"value":"foo"}]""")
        val defaultListWithOneValue: List<FooElement>,
    )

    @Suppress("ArrayInDataClass")
    @Serializable
    private data class BarArray(
        @AvroDefault("[]")
        val defaultEmptyArray: Array<String>,
        @AvroDefault("null")
        val nullableDefaultEmptyArray: Array<String>?,
        @AvroDefault("""["John", "Doe"]""")
        val defaultStringArrayWith2Defaults: Array<String>,
        @AvroDefault("""[1, 2]""")
        val defaultIntArrayWith2Defaults: Array<Int>,
        @AvroDefault("""[3.14, 9.89]""")
        val defaultFloatArrayWith2Defaults: Array<Float>,
        // Unions are currently not correctly supported by Java-Avro, so for now we do not test with null values in
        // the default
        // See https://issues.apache.org/jira/browse/AVRO-2647
        @AvroDefault("""[null]""")
        val defaultStringArrayWithNullableTypes: Array<String?>,
    )

    @Serializable
    private data class BarInvalidArrayType(
        @AvroDefault("""["foo-bar"]""")
        val defaultFloatArrayWith2Defaults: List<Float>,
    )

    @Serializable
    private data class BarInvalidNonArrayType(
        @AvroDefault("{}")
        val defaultBarArrayWithNonWorkingDefaults: List<Int> = ArrayList(),
    )
}