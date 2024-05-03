package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.AvroNamespaceOverride
import com.github.avrokotlin.avro4k.schema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import kotlin.io.path.Path

class AvroNamespaceOverrideSchemaTest : FunSpec({

    test("support namespace annotations on records") {
        val schema = Avro.schema<AnnotatedNamespace>()
        schema.namespace shouldBe "com.yuval"
    }

    test("support namespace annotations in nested records") {
        AvroAssertions.assertThat<NestedAnnotatedNamespace>()
            .generatesSchema(Path("/namespace.json"))
    }

    test("support namespace annotations on field") {
        AvroAssertions.assertThat<InternalAnnotatedNamespace>()
            .generatesSchema(Path("/namespace.json"))
    }

    test("favour namespace annotations on field over record") {
        AvroAssertions.assertThat<FieldAnnotatedNamespace>()
            .generatesSchema(Path("/namespace.json"))
    }

    test("favour namespace annotations on the topest value class field over record") {
        AvroAssertions.assertThat<NestedValueClasses>()
            .generatesSchema(Path("/namespace_nested.json"))
    }

    test("empty namespace") {
        AvroAssertions.assertThat<Foo>()
            .generatesSchema(Path("/namespace_empty.json"))
    }

    test("support @AvroNamespaceOverride on map") {
        val expected =
            SchemaBuilder.map().values()
                .record("AnnotatedNamespace").namespace("override").fields()
                .name("s").type().stringType().noDefault()
                .endRecord()

        AvroAssertions.assertThat<MapNsOverride>()
            .generatesSchema(expected)
    }

    test("support @AvroNamespaceOverride on array") {
        val expected =
            SchemaBuilder.array().items()
                .record("AnnotatedNamespace").namespace("override").fields()
                .name("s").type().stringType().noDefault()
                .endRecord()

        AvroAssertions.assertThat<ArrayNsOverride>()
            .generatesSchema(expected)
    }

    test("support @AvroNamespaceOverride on union") {
        val expected =
            Schema.createUnion(
                SchemaBuilder.record("One").namespace("override").fields()
                    .name("s").type().stringType().noDefault()
                    .endRecord(),
                SchemaBuilder.record("Two").namespace("override").fields().endRecord()
            )

        AvroAssertions.assertThat<UnionNsOverride>()
            .generatesSchema(expected)
    }
}) {
    @JvmInline
    @Serializable
    private value class MapNsOverride(
        @AvroNamespaceOverride("override") val value: Map<String, AnnotatedNamespace>,
    )

    @JvmInline
    @Serializable
    private value class UnionNsOverride(
        @AvroNamespaceOverride("override") val value: Root,
    )

    @Serializable
    private sealed interface Root {
        @Serializable
        data class One(val s: String) : Root

        @Serializable
        object Two : Root
    }

    @JvmInline
    @Serializable
    private value class ArrayNsOverride(
        @AvroNamespaceOverride("override") val value: List<AnnotatedNamespace>,
    )

    @SerialName("com.yuval.AnnotatedNamespace")
    @Serializable
    private data class AnnotatedNamespace(val s: String)

    @SerialName("com.yuval.internal.InternalAnnotated")
    @Serializable
    private data class InternalAnnotated(val i: Int)

    @SerialName("com.yuval.AnnotatedNamespace")
    @Serializable
    private data class NestedAnnotatedNamespace(val s: String, val internal: InternalAnnotated)

    @Serializable
    @SerialName("InternalAnnotated")
    private data class Internal(val i: Int)

    @Serializable
    @SerialName("com.yuval.AnnotatedNamespace")
    private data class InternalAnnotatedNamespace(
        val s: String,
        @AvroNamespaceOverride("com.yuval.internal") val internal: Internal,
    )

    @Serializable
    @SerialName("shouldbeignored.InternalAnnotated")
    private data class InternalIgnoreAnnotated(val i: Int)

    @Serializable
    @SerialName("com.yuval.AnnotatedNamespace")
    private data class FieldAnnotatedNamespace(
        val s: String,
        @AvroNamespaceOverride("com.yuval.internal") val internal: InternalIgnoreAnnotated,
    )

    @SerialName("Foo")
    @Serializable
    private data class Foo(val s: String)

    @SerialName("toto.NestedValueClasses")
    @Serializable
    private data class NestedValueClasses(val nested: Nested1)

    @JvmInline
    @SerialName("shouldbeignored.Nested1")
    @Serializable
    private value class Nested1(
        @AvroNamespaceOverride("primaryspace") val s: Nested2,
    )

    @JvmInline
    @SerialName("shouldbeignored.Nested2")
    @Serializable
    private value class Nested2(
        @AvroNamespaceOverride("shouldalsobeignored") val s: Nested3,
    )

    @SerialName("shouldbeignored.Nested3")
    @Serializable
    private data class Nested3(
        val s: String,
        @AvroNamespaceOverride("ignored") val enum: NestedEnum,
        @AvroFixed(42) val fixed: String,
    )

    @Serializable
    enum class NestedEnum {
        A,

        @AvroEnumDefault
        B,
    }
}