package com.github.avrokotlin.avro4k.io

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.schema.Level1
import com.sksamuel.avro4k.schema.Level2
import com.sksamuel.avro4k.schema.Level3
import com.sksamuel.avro4k.schema.Level4
import com.sksamuel.avro4k.schema.RecursiveClass
import com.sksamuel.avro4k.schema.RecursiveListItem
import com.sksamuel.avro4k.schema.RecursiveMapValue
import com.sksamuel.avro4k.schema.RecursivePair
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

class RecursiveIoTest : StringSpec({

   "read / write direct recursive class" {
      writeRead(RecursiveClass(1, RecursiveClass(2, null)), RecursiveClass.serializer())
      writeRead(RecursiveClass(1, RecursiveClass(2, null)), RecursiveClass.serializer()) {
         it["payload"] shouldBe 1
         it["klass"].shouldBeInstanceOf<GenericRecord>()
         val klass = it["klass"] as GenericRecord
         klass.schema shouldBe Avro.default.schema(RecursiveClass.serializer())
         klass["payload"] shouldBe 2
         klass["klass"] shouldBe null
      }
   }

   "read / write direct recursive list" {
      writeRead(RecursiveListItem(1, listOf(RecursiveListItem(2, null))), RecursiveListItem.serializer())
      writeRead(RecursiveListItem(1, listOf(RecursiveListItem(2, null))), RecursiveListItem.serializer()) {
         it["payload"] shouldBe 1
         it["list"].shouldBeInstanceOf<List<GenericRecord>>()
         val list = (it["list"] as List<*>)[0] as GenericRecord
         list.schema shouldBe Avro.default.schema(RecursiveListItem.serializer())
         list["payload"] shouldBe 2
         list["list"] shouldBe null
      }
   }

   "read / write direct recursive map" {
      writeRead(RecursiveMapValue(1, mapOf("a" to RecursiveMapValue(2, null))), RecursiveMapValue.serializer())
      writeRead(RecursiveMapValue(1, mapOf("a" to RecursiveMapValue(2, null))), RecursiveMapValue.serializer()) {
         it["payload"] shouldBe 1
         it["map"].shouldBeInstanceOf<Map<String, GenericRecord>>()
         val map = (it["map"] as Map<*, *>)[Utf8("a")]!! as GenericRecord
         map.schema shouldBe Avro.default.schema(RecursiveMapValue.serializer())
         map["payload"] shouldBe 2
         map["map"] shouldBe null
      }
   }

   "read / write direct recursive pair" {
      writeRead(RecursivePair(1, (RecursivePair(2, null) to RecursivePair(3, null))), RecursivePair.serializer())
      writeRead(RecursivePair(1, (RecursivePair(2, null) to RecursivePair(3, null))), RecursivePair.serializer()) {
         it["payload"] shouldBe 1
         it["pair"].shouldBeInstanceOf<GenericData.Record>()
         val first = (it["pair"] as GenericData.Record)["first"]
         first.shouldBeInstanceOf<GenericRecord>()
         first.schema shouldBe Avro.default.schema(RecursivePair.serializer())
         first["payload"] shouldBe 2
         first["pair"] shouldBe null
         val second = (it["pair"] as GenericData.Record)["second"]
         second.shouldBeInstanceOf<GenericRecord>()
         second.schema shouldBe Avro.default.schema(RecursivePair.serializer())
         second["payload"] shouldBe 3
         second["pair"] shouldBe null
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
