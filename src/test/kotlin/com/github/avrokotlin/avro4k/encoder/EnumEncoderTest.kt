package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.encode
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.avro4k.schema.Wine
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData

class EnumEncoderTest : FunSpec({
    test("encode enum value") {
        val schema = Avro.default.schema<Wine>()
        Avro.default.encode(schema, Wine.Malbec) shouldBe
                GenericData.EnumSymbol(schema, "Malbec")
    }

    test("encode enum field") {
        val enumSchema = Avro.default.schema<Wine>()
        Avro.default.encode(MyEnum(Wine.Malbec, Wine.Merlot, null)) shouldBeContentOf
                ListRecord(Avro.default.schema<MyEnum>(), GenericData.EnumSymbol(enumSchema, "Malbec"), GenericData.EnumSymbol(enumSchema, "Merlot"), null)
    }
}) {
    @Serializable
    data class MyEnum(val a: Wine, val b: Wine?, val c: Wine?)
}