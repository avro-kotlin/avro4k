package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.schema.ValueClassSchemaTest
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.spec.style.stringSpec
import java.util.*

fun valueClassEncoderTests(encoderToTest: EncoderToTest): TestFactory {
    return stringSpec {
        "encode value class" {
            val uuid = UUID.randomUUID()
            encoderToTest.testEncodeDecode(
                ValueClassSchemaTest.ContainsInlineTest(
                    ValueClassSchemaTest.StringWrapper("100500"), ValueClassSchemaTest.UuidWrapper(uuid)
                ),
                record("100500", uuid.toString())
            )
        }
    }
}

class ValueClassEncoderTest : StringSpec({
    includeForEveryEncoder { valueClassEncoderTests(it) }
})