package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroFixed
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class AvroFixedSchemaTest : WordSpec({

    "@AvroFixed" should {

        "generated fixed field schema when used on a field" {

            val schema = Avro.default.schema(FixedStringField.serializer())
            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/fixed_string.json"))
            schema.toString(true) shouldBe expected.toString(true)
        }
    }
}) {
    @Serializable
    data class FixedStringField(
        @AvroFixed(7) val mystring: String,
    )
}