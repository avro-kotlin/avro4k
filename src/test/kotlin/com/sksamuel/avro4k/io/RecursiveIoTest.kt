package com.sksamuel.avro4k.io

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.schema.Level1
import com.sksamuel.avro4k.schema.Level2
import com.sksamuel.avro4k.schema.Level3
import com.sksamuel.avro4k.schema.Level4
import com.sksamuel.avro4k.schema.Recursive
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.avro.generic.GenericRecord

class RecursiveIoTest : StringSpec({

   "read / write direct recursive class" {
      writeRead(Recursive(1, Recursive(2, null)), Recursive.serializer())
      writeRead(Recursive(1, Recursive(2, null)), Recursive.serializer()) {
         it["payload"] shouldBe 1
         it["next"].shouldBeInstanceOf<GenericRecord>()
         val next = it["next"] as GenericRecord
         next.schema shouldBe Avro.default.schema(Recursive.serializer())
         next["payload"] shouldBe 2
         next["next"] shouldBe null
      }
   }

   "read / write nested recursive classes" {
      writeRead(Level1(Level2(Level3(Level4(Level1(null))))), Level1.serializer())
      writeRead(Level1(Level2(Level3(Level4(Level1(null))))), Level1.serializer()) {
         it["level2"].shouldBeInstanceOf<GenericRecord>()
         val level2 = it["level2"] as GenericRecord
         level2["level3"].shouldBeInstanceOf<GenericRecord>()
         val level3 = level2["level3"] as GenericRecord
         level3["level4"].shouldBeInstanceOf<GenericRecord>()
         val level4 = level3["level4"] as GenericRecord
         level4["level1"].shouldBeInstanceOf<GenericRecord>()
         val level1 = level4["level1"] as GenericRecord
         level1["level2"] shouldBe null
      }
   }
})
