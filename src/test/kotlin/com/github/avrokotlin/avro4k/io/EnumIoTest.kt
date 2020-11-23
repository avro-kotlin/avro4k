@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.io

import com.sksamuel.avro4k.serializer.UUIDSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol

enum class Cream {
   Bruce, Baker, Clapton
}

enum class BBM {
   Bruce, Baker, Moore
}

class EnumIoTest : StringSpec({

   "read / write enums" {

      @Serializable
      data class EnumTest(val a: Cream, val b: BBM)

      writeRead(EnumTest(Cream.Bruce, BBM.Moore), EnumTest.serializer())
      writeRead(EnumTest(Cream.Bruce, BBM.Moore), EnumTest.serializer()) {
         (it["a"] as GenericEnumSymbol<*>).toString() shouldBe "Bruce"
         (it["b"] as GenericEnumSymbol<*>).toString() shouldBe "Moore"
      }
   }

   "read / write list of enums" {

      @Serializable
      data class EnumTest(val a: List<Cream>)

      writeRead(EnumTest(listOf(Cream.Bruce, Cream.Clapton)), EnumTest.serializer())
      writeRead(EnumTest(listOf(Cream.Bruce, Cream.Clapton)), EnumTest.serializer()) { record ->
         (record["a"] as List<*>).map { it.toString() } shouldBe listOf("Bruce", "Clapton")
      }
   }

   "read / write nullable enums" {

      @Serializable
      data class EnumTest(val a: Cream?)

      writeRead(EnumTest(null), EnumTest.serializer())
      writeRead(EnumTest(Cream.Bruce), EnumTest.serializer())
      writeRead(EnumTest(Cream.Bruce), EnumTest.serializer()) {
         (it["a"] as GenericData.EnumSymbol).toString() shouldBe "Bruce"
      }
   }
})