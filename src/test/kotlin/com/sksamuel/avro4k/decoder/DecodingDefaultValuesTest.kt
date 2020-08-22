@file:UseSerializers(
   LocalDateSerializer::class
)
package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.serializer.LocalDateSerializer
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.time.LocalDate

class DecodingDefaultValuesTest : FunSpec({
   test("data class decoder should override defaults with avro data") {
      val foo = Foo("a", "b", LocalDate.of(2019, 1, 2), LocalDate.of(2018, 4, 5))
      val bytes = Avro.default.encodeToByteArray(Foo.serializer(), foo)
      Avro.default.decodeFromByteArray(Foo.serializer(), bytes) shouldBe foo
   }
})

@Serializable
data class Foo(
   val a: String,
   val b: String = "hello",
   val c: LocalDate = LocalDate.of(1979, 9, 10),
   val d: LocalDate
)