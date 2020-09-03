package com.sksamuel.avro4k.io

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.schema.Operation
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericRecord

class SealedClassIoTest : StringSpec({

   "read / write sealed class" {

      @Serializable
      data class SealedClassTest(val a: Operation)

      writeRead(SealedClassTest(Operation.Unary.Negate(1)), SealedClassTest.serializer())
      writeRead(SealedClassTest(Operation.Unary.Negate(1)), SealedClassTest.serializer()) {
         val operation = it["a"] as GenericRecord
         operation.schema shouldBe Avro.default.schema(Operation.Unary.Negate.serializer())
         operation["value"] shouldBe 1
      }
   }

   "read / write list of sealed class values" {

      @Serializable
      data class SealedClassTest(val a: List<Operation>)

      writeRead(SealedClassTest(listOf(Operation.Unary.Negate(1),Operation.Binary.Add(3,4))), SealedClassTest.serializer())
      writeRead(SealedClassTest(listOf(Operation.Unary.Negate(1),Operation.Binary.Add(3,4))), SealedClassTest.serializer()) {
         it["a"].shouldBeInstanceOf<List<GenericRecord>>()
         @Suppress("UNCHECKED_CAST")
         val operations = it["a"] as List<GenericRecord>
         operations.size shouldBe 2
         operations[0].schema shouldBe Avro.default.schema(Operation.Unary.Negate.serializer())
         operations[1].schema shouldBe Avro.default.schema(Operation.Binary.Add.serializer())

         operations[0]["value"] shouldBe 1
         operations[1]["left"] shouldBe 3
         operations[1]["right"] shouldBe 4
      }
   }

   "read / write nullable sealed class" {

      @Serializable
      data class SealedClassTest(val a: Operation?)

      writeRead(SealedClassTest(null), SealedClassTest.serializer())
      writeRead(SealedClassTest(Operation.Unary.Negate(1)), SealedClassTest.serializer())
      writeRead(SealedClassTest(Operation.Unary.Negate(1)), SealedClassTest.serializer()) {
         val operation = it["a"] as GenericRecord
         operation.schema shouldBe Avro.default.schema(Operation.Unary.Negate.serializer())
         operation["value"] shouldBe 1
      }
   }
   "read / wirte sealed class directly"{
      writeRead(Operation.Unary.Negate(1), Operation.serializer())
      writeRead(Operation.Unary.Negate(1), Operation.serializer()) {operation ->
         operation.schema shouldBe Avro.default.schema(Operation.Unary.Negate.serializer())
         operation["value"] shouldBe 1
      }
   }
})