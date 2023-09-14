@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.util.*

fun uuidEncoderTests(encoderToTest: EncoderToTest): TestFactory {
    return funSpec {
        test("encode uuids") {
            @Serializable
            data class UUIDTest(val uuid: UUID)

            val uuid = UUID.randomUUID()
            encoderToTest.testEncodeDecode(UUIDTest(uuid), record(uuid.toString()))
        }

        test("encode nullable uuids") {
            @Serializable
            data class NullableUUIDTest(val uuid: UUID?)

            val uuid = UUID.randomUUID()
            encoderToTest.testEncodeDecode(NullableUUIDTest(uuid), record(uuid.toString()))
            encoderToTest.testEncodeDecode(NullableUUIDTest(null), record(null))
        }
    }
}

class UUIDEncoderTest : FunSpec({
    includeForEveryEncoder { uuidEncoderTests(it) }
}) {

}
