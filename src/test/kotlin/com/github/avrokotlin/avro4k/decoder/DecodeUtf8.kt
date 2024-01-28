package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class DecodeUtf8 : StringSpec({
    "decode utf8" {
        @Serializable
        data class Foo(val a: String)

        val schema = Avro.default.schema(Foo.serializer())

        val record = GenericData.Record(schema)
        record.put("a", Utf8("utf8-string"))
        Avro.default.fromRecord(Foo.serializer(), record) shouldBe Foo("utf8-string")
    }
})