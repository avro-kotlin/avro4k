@file:UseSerializers(
   LocalDateSerializer::class
)
package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import java.time.LocalDate
import java.time.temporal.TemporalField

class DecodingDefaultValuesTest : FunSpec({
   test("data class decoder should override defaults with avro data") {
      val foo = Foo("a", "b", LocalDate.of(2019, 1, 2), LocalDate.of(2018, 4, 5))
      val bytes = Avro.default.encodeToByteArray(Foo.serializer(), foo)
      Avro.default.decodeFromByteArray(Foo.serializer(), bytes) shouldBe foo
   }

   test("in case of missing optional field, it should use default value") {
      val foo = Foo("a", d = LocalDate.of(2018, 4, 5))
      val record = Avro.default.toRecord(Foo.serializer(), foo)
      val schemaWithoutBandC = Schema.createRecord(
         "Foo", null, "com.github.avrokotlin.avro4k.decoder", false,
         listOf(
            Schema.Field("a", Schema.create(Schema.Type.STRING)),
            Schema.Field("d", LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))),
         )
      )
      val recordWithoutBandC = ListRecord(
         schemaWithoutBandC,
         listOf(
            Utf8("a"),
            foo.d.toEpochDay()
         )
      )
      Avro.default.fromRecord(Foo.serializer(), recordWithoutBandC) shouldBe foo
   }
})

@Serializable
data class Foo(
   val a: String,
   val b: String = "hello",
   val c: LocalDate = LocalDate.of(1979, 9, 10),
   val d: LocalDate
)