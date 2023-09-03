package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroName
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class AvroNameEncoderTest : FunSpec({

   test("encoder should take into account @AvroName on fields") {
       val schema = Avro.default.schema(Foo.serializer())
       Avro.default.encode(Foo.serializer(), Foo("hello")) shouldBeContentOf ListRecord(schema, listOf(Utf8("hello")))
   }
}) {
   @Serializable
   data class Foo(@AvroName("bar") val foo: String)
}
