package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.decoder.MyWine
import com.github.avrokotlin.avro4k.decoder.NullableWine
import com.github.avrokotlin.avro4k.schema.Wine
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import org.apache.avro.generic.GenericData

class EnumEncoderTest : FunSpec({

   test("support enums") {
      val schema = Avro.default.schema(MyWine.serializer())
      Avro.default.toRecord(MyWine.serializer(), schema, MyWine(Wine.Malbec)) shouldBe
         ListRecord(
            schema,
            GenericData.EnumSymbol(schema.getField("wine").schema(), "Malbec")
         )
   }

   test("support nullable enums") {
      val schema = Avro.default.schema(NullableWine.serializer())

      val record1 = GenericData.Record(schema)
      record1.put("wine", GenericData.EnumSymbol(schema.getField("wine").schema(), "Shiraz"))
      Avro.default.fromRecord(NullableWine.serializer(), record1) shouldBe NullableWine(Wine.Shiraz)

      val record2 = GenericData.Record(schema)
      record2.put("wine", null)
      Avro.default.fromRecord(NullableWine.serializer(), record2) shouldBe NullableWine(null)
   }
})