package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.WrappedInt
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.schema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encodeToByteArray
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

internal class ImplicitConfigSchemaTest : FunSpec({
    test("Should set default value to null for nullable fields when implicitNulls is true (default)") {
        AvroAssertions.assertThat<ImplicitNulls>()
            .generatesSchema(
                SchemaBuilder.record("ImplicitNulls").fields()
                    .name("string").type(Schema.create(Schema.Type.STRING).nullable).withDefault(null)
                    .name("boolean").type(Schema.create(Schema.Type.BOOLEAN).nullable).withDefault(null)
                    .name("booleanWrapped1").type(Schema.create(Schema.Type.BOOLEAN).nullable).withDefault(null)
                    .name("intWrapped").type(Schema.create(Schema.Type.INT).nullable).withDefault(null)
                    .name("doubleWrapped").type(Schema.create(Schema.Type.DOUBLE).nullable).withDefault(null)
                    .name("nested").type(
                        SchemaBuilder.record("Nested").fields()
                            .name("string").type(Schema.create(Schema.Type.STRING).nullable).withDefault(null)
                            .name("boolean").type(Schema.create(Schema.Type.BOOLEAN).nullable).withDefault(null)
                            .endRecord().nullable
                    ).withDefault(null)
                    .name("nullableList").type(Schema.createArray(Schema.create(Schema.Type.STRING)).nullable).withDefault(null)
                    .name("nullableMap").type(Schema.createMap(Schema.create(Schema.Type.STRING)).nullable).withDefault(null)
                    .name("stringWithAvroDefault").type().nullable().stringType().stringDefault("implicit nulls bypassed")
                    .endRecord()
            )
        AvroAssertions.assertThat(EmptyType)
            .isDecodedAs(
                ImplicitNulls(
                    string = null,
                    boolean = null,
                    booleanWrapped1 = NullableBooleanWrapper(null),
                    intWrapped = null,
                    doubleWrapped = null,
                    nested = null,
                    nullableList = null,
                    nullableMap = null,
                    stringWithAvroDefault = "implicit nulls bypassed"
                )
            )
    }

    test("Should fail for missing nullable fields when implicitNulls is false. Collections are still having their implicit default") {
        AvroAssertions.assertThat<ImplicitNulls>()
            .withConfig {
                implicitNulls = false
            }
            .generatesSchema(
                SchemaBuilder.record("ImplicitNulls").fields()
                    .name("string").type(Schema.create(Schema.Type.STRING).nullable).noDefault()
                    .name("boolean").type(Schema.create(Schema.Type.BOOLEAN).nullable).noDefault()
                    .name("booleanWrapped1").type(Schema.create(Schema.Type.BOOLEAN).nullable).noDefault()
                    .name("intWrapped").type(Schema.create(Schema.Type.INT).nullable).noDefault()
                    .name("doubleWrapped").type(Schema.create(Schema.Type.DOUBLE).nullable).noDefault()
                    .name("nested").type(
                        SchemaBuilder.record("Nested").fields()
                            .name("string").type(Schema.create(Schema.Type.STRING).nullable).noDefault()
                            .name("boolean").type(Schema.create(Schema.Type.BOOLEAN).nullable).noDefault()
                            .endRecord().nullable
                    ).noDefault()
                    .name("nullableList").type().nullable().array().items().stringType().arrayDefault(emptyList<Any>())
                    .name("nullableMap").type().nullable().map().values().stringType().mapDefault(emptyMap<Any, Any>())
                    .name("stringWithAvroDefault").type().nullable().stringType().stringDefault("implicit nulls bypassed")
                    .endRecord()
            )
        val avro =
            Avro {
                implicitNulls = false
            }
        val bytes = avro.encodeToByteArray(EmptyType)
        shouldThrow<SerializationException> {
            avro.decodeFromByteArray<ImplicitNulls>(writerSchema = avro.schema<EmptyType>(), bytes)
        }
    }

    test("Should set default value to empty array for Collection fields when implicitEmptyCollections is true (default)") {
        AvroAssertions.assertThat<ImplicitEmptyCollections>()
            .generatesSchema(
                SchemaBuilder.record("ImplicitEmptyCollections").fields()
                    .name("list").type().array().items().stringType().arrayDefault<Any>(emptyList())
                    .name("set").type().array().items().stringType().arrayDefault<Any>(emptyList())
                    .name("collection").type().array().items().stringType().arrayDefault<Any>(emptyList())
                    .name("map").type().map().values().stringType().mapDefault<Any, Any>(emptyMap())
                    .endRecord()
            )
        AvroAssertions.assertThat(EmptyCollectionType)
            .isDecodedAs(
                ImplicitEmptyCollections(
                    list = emptyList(),
                    set = emptySet(),
                    collection = emptyList(),
                    map = emptyMap()
                )
            )
    }

    test("Should not set default value for Collection fields when implicitEmptyCollections is false") {
        AvroAssertions.assertThat<ImplicitEmptyCollections>()
            .withConfig {
                implicitEmptyCollections = false
            }
            .generatesSchema(
                SchemaBuilder.record("ImplicitEmptyCollections").fields()
                    .name("list").type().array().items().stringType().noDefault()
                    .name("set").type().array().items().stringType().noDefault()
                    .name("collection").type().array().items().stringType().noDefault()
                    .name("map").type().map().values().stringType().noDefault()
                    .endRecord()
            )
        val avro =
            Avro {
                implicitEmptyCollections = false
            }
        val bytes = avro.encodeToByteArray(EmptyCollectionType)
        shouldThrow<SerializationException> {
            avro.decodeFromByteArray<ImplicitEmptyCollections>(writerSchema = avro.schema<EmptyCollectionType>(), bytes)
        }
    }
}) {
    @Serializable
    @SerialName("ImplicitNulls")
    private data object EmptyType

    @Serializable
    @SerialName("ImplicitNulls")
    private data class ImplicitNulls(
        val string: String?,
        val boolean: Boolean?,
        val booleanWrapped1: NullableBooleanWrapper,
        val intWrapped: WrappedInt?,
        val doubleWrapped: NullableDoubleWrapper?,
        val nested: Nested?,
        val nullableList: List<String>?,
        val nullableMap: Map<String, String>?,
        @AvroDefault("implicit nulls bypassed")
        val stringWithAvroDefault: String?,
    )

    @Serializable
    @SerialName("ImplicitEmptyCollections")
    private data object EmptyCollectionType

    @Serializable
    @SerialName("ImplicitEmptyCollections")
    private data class ImplicitEmptyCollections(
        val list: List<String>,
        val set: Set<String>,
        val collection: Collection<String>,
        val map: Map<String, String>,
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