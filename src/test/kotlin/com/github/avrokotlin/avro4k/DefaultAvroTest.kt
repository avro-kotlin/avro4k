package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import java.util.UUID

class DefaultAvroTest : FunSpec({

    test("encoding UUID") {
        val uuid = UUID.randomUUID()
        val record = Avro.default.toRecord(Foo.serializer(), Foo(uuid))
        record.get("a").toString() shouldBe uuid.toString()
    }
}) {
    @Serializable
    data class Foo(
        @Contextual val a: UUID,
    )
}