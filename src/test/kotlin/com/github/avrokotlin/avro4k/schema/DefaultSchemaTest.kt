@file:UseSerializers(
   TimestampSerializer::class
)

package com.github.avrokotlin.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.serializer.TimestampSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.sql.Timestamp
import java.time.Instant

class DefaultSchemaTest : FunSpec() {
   init {
      test("schema for data class should include fields that define a default") {
         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/default.json"))
         val schema = Avro.default.schema(Foo.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }
   }
}

@Serializable
data class Foo(
   val a: String,
   val b: String = "hello",
   val c: Timestamp = Timestamp.from(Instant.now()),
   val d: Timestamp
)