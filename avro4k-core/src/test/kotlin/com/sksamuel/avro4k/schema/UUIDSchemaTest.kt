@file:UseSerializers(UUIDSerializer::class)

package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.serializer.UUIDSerializer
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.util.*

class UUIDSchemaTest : FunSpec({

   test("support UUID logical types") {

      @Serializable
      data class UUIDTest(val uuid: UUID)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/uuid.json"))
      val schema = Avro.default.schema(UUIDTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

})