package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import java.util.UUID

class DefaultAvroTest : FunSpec({

    test("encoding UUID") {
        val uuid = UUID.randomUUID()
        val record = Avro.default.encode(Foo.serializer(), Foo(uuid))
        record shouldBeContentOf ListRecord(Avro.default.schema(Foo.serializer()), uuid.toString())
    }
}) {
    @Serializable
    data class Foo(@Contextual val a: UUID)
}
