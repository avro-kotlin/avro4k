@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.util.*

class UUIDEncoderTest : FunSpec({
    includeForEveryEncoder { uuidEncoderTests(it) }
})
fun uuidEncoderTests(encoderToTest: EnDecoder<*>): TestFactory {
    return stringSpec {
        "encode/decode UUIDs" {
            @Serializable
            data class UUIDTest(val uuid: UUID)

            val uuid = UUID.randomUUID()
            encoderToTest.testEncodeDecode(UUIDTest(uuid), record(uuid.toString()))
        }

        "encode/decode nullable UUIDs" {
            @Serializable
            data class NullableUUIDTest(val uuid: UUID?)

            val uuid = UUID.randomUUID()
            encoderToTest.testEncodeDecode(NullableUUIDTest(uuid), record(uuid.toString()))
            encoderToTest.testEncodeDecode(NullableUUIDTest(null), record(null))
        }
    }
}