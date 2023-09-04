package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.schema.ValueClassSchemaTest
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8
import java.util.UUID

class ValueClassEncoderTest : StringSpec({
    "encode value class" {
        val id = ValueClassSchemaTest.StringWrapper("100500")
        val uuid = UUID.randomUUID()
        val uuidStr = uuid.toString()
        val uuidW = ValueClassSchemaTest.UuidWrapper(uuid)
        val schema = Avro.default.schema(ValueClassSchemaTest.ContainsInlineTest.serializer())
        Avro.default.encodeToGenericData(ValueClassSchemaTest.ContainsInlineTest.serializer(),
            ValueClassSchemaTest.ContainsInlineTest(id, uuidW)) shouldBeContentOf ListRecord(schema, Utf8(id.a), Utf8(uuidStr))
    }

    "encode value class even if inside a polymorphic type" {
        val schema = Avro.default.schema(Parent.serializer())
        val record = Avro.default.encodeToGenericData<Parent>(Product("123", Name("sneakers")))
        record shouldBeContentOf ListRecord(schema.types[0], listOf(Utf8("123"), Utf8("sneakers")))
    }
}) {
    @JvmInline
    @Serializable
    value class Name(val value: String)

    @Serializable
    sealed interface Parent

    @Serializable
    data class Product(val id: String, val name: Name) : Parent
}
