package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema.ValueClassSchemaTest
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.spec.style.stringSpec
import java.util.*

class ValueClassEncoderTest : StringSpec({
    includeForEveryEncoder { valueClassEncoderTests(it) }
})
fun valueClassEncoderTests(encoderToTest: EnDecoder): TestFactory {
    return stringSpec {
        "encode/decode value class" {
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