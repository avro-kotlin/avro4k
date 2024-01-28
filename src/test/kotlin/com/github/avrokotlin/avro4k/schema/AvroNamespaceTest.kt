package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroInline
import com.github.avrokotlin.avro4k.AvroNamespaceOverride
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

class AvroNamespaceSchemaTest : FunSpec({

    test("support namespace annotations on records") {

        val schema = Avro.default.schema(AnnotatedNamespace.serializer())
        schema.namespace shouldBe "com.yuval"
    }

    test("support namespace annotations in nested records") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/namespace.json"))
        val schema = Avro.default.schema(NestedAnnotatedNamespace.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("support namespace annotations on field") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/namespace.json"))
        val schema = Avro.default.schema(InternalAnnotatedNamespace.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("favour namespace annotations on field over record") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/namespace.json"))
        val schema = Avro.default.schema(FieldAnnotatedNamespace.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("favour namespace annotations on the topest value class field over record") {
        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/namespace_nested.json"))
        val schema = Avro.default.schema(NestedValueClasses.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("empty namespace") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/namespace_empty.json"))
        val schema = Avro.default.schema(Foo.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }
}) {
    @SerialName("com.yuval.AnnotatedNamespace")
    @Serializable
    data class AnnotatedNamespace(val s: String)

    @SerialName("com.yuval.internal.InternalAnnotated")
    @Serializable
    data class InternalAnnotated(val i: Int)

    @SerialName("com.yuval.AnnotatedNamespace")
    @Serializable
    data class NestedAnnotatedNamespace(val s: String, val internal: InternalAnnotated)

    @Serializable
    @SerialName("InternalAnnotated")
    data class Internal(val i: Int)

    @Serializable
    @SerialName("com.yuval.AnnotatedNamespace")
    data class InternalAnnotatedNamespace(
        val s: String,
        @AvroNamespaceOverride("com.yuval.internal") val internal: Internal,
    )

    @Serializable
    @SerialName("shouldbeignored.InternalAnnotated")
    data class InternalIgnoreAnnotated(val i: Int)

    @Serializable
    @SerialName("com.yuval.AnnotatedNamespace")
    data class FieldAnnotatedNamespace(
        val s: String,
        @AvroNamespaceOverride("com.yuval.internal") val internal: InternalIgnoreAnnotated,
    )

    @SerialName("Foo")
    @Serializable
    data class Foo(val s: String)

    @SerialName("toto.NestedValueClasses")
    @Serializable
    data class NestedValueClasses(val nested: Nested1)

    @AvroInline
    @SerialName("shouldbeignored.Nested1")
    @Serializable
    data class Nested1(
        @AvroNamespaceOverride("primaryspace") val s: Nested2,
    )

    @AvroInline
    @SerialName("shouldbeignored.Nested2")
    @Serializable
    data class Nested2(
        @AvroNamespaceOverride("shouldalsobeignored") val s: Nested3,
    )

    @SerialName("shouldbeignored.Nested3")
    @Serializable
    data class Nested3(val s: String)
}