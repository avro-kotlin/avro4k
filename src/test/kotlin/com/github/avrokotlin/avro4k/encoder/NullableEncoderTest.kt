package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.decoder.NullableBoolean
import com.github.avrokotlin.avro4k.decoder.NullableString
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import org.apache.avro.util.Utf8

class NullableEncoderTest : FunSpec({

   test("encode nullable strings") {
       val schema = Avro.default.schema(NullableString.serializer())

       Avro.default.encodeToGenericData(NullableString("hello")) shouldBeContentOf
               ListRecord(schema, Utf8("hello"))

       Avro.default.encodeToGenericData(NullableString(null)) shouldBeContentOf
               ListRecord(schema, null)
   }

   test("encode nullable booleans") {
       val schema = Avro.default.schema(NullableBoolean.serializer())

       Avro.default.encodeToGenericData(NullableBoolean(true)) shouldBeContentOf
               ListRecord(schema, true)

       Avro.default.encodeToGenericData(NullableBoolean(false)) shouldBeContentOf
               ListRecord(schema, false)

       Avro.default.encodeToGenericData(NullableBoolean(null)) shouldBeContentOf
               ListRecord(schema, null)
   }
})