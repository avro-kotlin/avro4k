package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema.PascalCaseNamingStrategy
import com.github.avrokotlin.avro4k.schema.SnakeCaseNamingStrategy
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.WordSpec
import io.kotest.core.spec.style.stringSpec
import io.kotest.matchers.shouldNotBe
import kotlinx.serialization.Serializable

class NamingStrategyEncoderTest : WordSpec({
    includeForEveryEncoder { namingStrategyEncoderTests(it) }
})

fun namingStrategyEncoderTests(enDecoder: EnDecoder): TestFactory {
    return stringSpec {
        @Serializable
        data class Foo(val fooBar: String)

        "encode/decode fields with snake_casing" {
            enDecoder.avro = Avro(AvroConfiguration(SnakeCaseNamingStrategy))
            val schema = enDecoder.avro.schema(Foo.serializer())
            schema.getField("foo_bar") shouldNotBe null
            enDecoder.testEncodeDecode(Foo("hello"), record("hello"))
        }

        "encode/decode fields with PascalCasing" {
            enDecoder.avro = Avro(AvroConfiguration(PascalCaseNamingStrategy))
            val schema = enDecoder.avro.schema(Foo.serializer())
            schema.getField("FooBar") shouldNotBe null
            enDecoder.testEncodeDecode(Foo("hello"), record("hello"))
        }
    }
}