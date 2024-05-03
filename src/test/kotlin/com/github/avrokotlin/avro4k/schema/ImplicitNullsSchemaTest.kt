package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.WrappedInt
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

internal class ImplicitNullsSchemaTest : FunSpec({
    test("Should set default value to null for nullable fields when implicitNulls is true") {
        AvroAssertions.assertThat<ImplicitNulls>()
            .withConfig { implicitNulls = true }
            .generatesSchema(Path("/nullables-with-defaults.json"))
    }
}) {
    @Serializable
    @SerialName("ImplicitNulls")
    private data class ImplicitNulls(
        val string: String?,
        val boolean: Boolean?,
        val booleanWrapped1: NullableBooleanWrapper,
        val booleanWrapped2: WrappedInt?,
        val booleanWrapped3: NullableDoubleWrapper?,
        val nested: Nested?,
        @AvroDefault("implicit nulls bypassed")
        val stringWithAvroDefault: String?,
    )

    @JvmInline
    @Serializable
    private value class NullableBooleanWrapper(val value: Boolean?)

    @JvmInline
    @Serializable
    private value class NullableDoubleWrapper(val value: Double?)

    @Serializable
    @SerialName("Nested")
    private data class Nested(
        val string: String?,
        val boolean: Boolean?,
    )
}