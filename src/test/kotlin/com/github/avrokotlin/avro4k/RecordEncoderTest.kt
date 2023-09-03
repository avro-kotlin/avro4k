package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

class RecordEncoderTest : FunSpec({
    test("encoding basic data class") {
        val input = Foo(
            "string value",
            2.2,
            true,
            S(setOf(1, 2, 3)),
            ValueClass(listOf(1, 2, 3))
        )
        val record = Avro.default.encode(input)
        val output = Avro.default.decode<Foo>(record)
        output shouldBe input
    }
    test("encoding basic data class with different field ordering than schema") {
        val schema = SchemaBuilder.record("Foo").fields()
            .requiredBoolean("c")
            .optionalDouble("b")
            .name("vc").type().bytesType().noDefault()
            .name("s").type().record("S").fields().name("t").type().array().items().type(Schema.create(Schema.Type.INT)).noDefault().endRecord().noDefault()
            .requiredString("a")
            .endRecord()
        val input = Foo(
            "string value",
            2.2,
            true,
            S(setOf(1, 2, 3)),
            ValueClass(listOf(1, 2, 3))
        )
        val record = Avro.default.encode(input)
        val output = Avro.default.decode<Foo>(record)
        output shouldBe input
    }
}) {
    @Serializable
    @SerialName("Foo")
    data class Foo(val a: String, val b: Double?, val c: Boolean, val s: S, val vc: ValueClass)

    @Serializable
    @SerialName("S")
    data class S(val t: Set<Int>)

    @JvmInline
    @Serializable
    value class ValueClass(val value: List<Byte>)
}
