package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroName
import com.github.avrokotlin.avro4k.AvroNamespace
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
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

    test("empty namespace") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/namespace_empty.json"))
        val schema = Avro.default.schema(Foo.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }
}) {
    @AvroNamespace("com.yuval")
    @Serializable
    data class AnnotatedNamespace(val s: String)

    @AvroNamespace("com.yuval.internal")
    @Serializable
    data class InternalAnnotated(val i: Int)

    @AvroName("AnnotatedNamespace")
    @AvroNamespace("com.yuval")
    @Serializable
    data class NestedAnnotatedNamespace(val s: String, val internal: InternalAnnotated)

    @Serializable
    @AvroName("InternalAnnotated")
    data class Internal(val i: Int)

    @Serializable
    @AvroName("AnnotatedNamespace")
    @AvroNamespace("com.yuval")
    data class InternalAnnotatedNamespace(
        val s: String,
        @AvroNamespace("com.yuval.internal") val internal: Internal,
    )

    @Serializable
    @AvroName("InternalAnnotated")
    @AvroNamespace("ignore")
    data class InternalIgnoreAnnotated(val i: Int)

    @Serializable
    @AvroName("AnnotatedNamespace")
    @AvroNamespace("com.yuval")
    data class FieldAnnotatedNamespace(
        val s: String,
        @AvroNamespace("com.yuval.internal") val internal: InternalIgnoreAnnotated,
    )

    @AvroNamespace("")
    @Serializable
    data class Foo(val s: String)
}