package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.record
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import org.apache.avro.generic.GenericData

class TransientEncoderTest : FunSpec({
    includeForEveryEncoder { transientEncoderTests(it) }
})

fun transientEncoderTests(encoderToTest: EnDecoder): TestFactory {
    @Serializable
    data class Foo(
        val a: String,
        @Transient val b: String = "foo",
        val c: String,
    )
    return stringSpec {
        "should skip @Transient fields" {
            val value = Foo("a", "b", "c")
            encoderToTest.testEncodeIsEqual(value, record("a", "c"))
        }
        "decoder should populate transient fields with default" {
            val schema = Avro.default.schema(Foo.serializer())
            val record = GenericData.Record(schema)
            record.put("a", "hello")

            val encoded = encoderToTest.testEncodeIsEqual(Foo("a", "b", "c"), record("a", "c"))
            encoderToTest.testDecodeIsEqual(encoded, Foo(a = "a", c = "c"))
        }
    }
}