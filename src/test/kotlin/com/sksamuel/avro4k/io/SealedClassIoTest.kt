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

   "read / write sealed class using object" {

      @Serializable
      data class SealedClassTest(val a: Operation)

      writeRead(SealedClassTest(Operation.Nullary), SealedClassTest.serializer())
      writeRead(SealedClassTest(Operation.Nullary), SealedClassTest.serializer()) {
         val operation = it["a"] as GenericRecord
         operation.schema shouldBe Avro.default.schema(Operation.Nullary.serializer())
      }
   }

   "read / write list of sealed class values" {

      @Serializable
      data class SealedClassTest(val a: List<Operation>)

      val test = SealedClassTest(listOf(
         Operation.Nullary,
         Operation.Unary.Negate(1),
         Operation.Binary.Add(3,4)
      ))
      writeRead(test, SealedClassTest.serializer())
      writeRead(test, SealedClassTest.serializer()) {
         it["a"].shouldBeInstanceOf<List<GenericRecord>>()
         @Suppress("UNCHECKED_CAST")
         val operations = it["a"] as List<GenericRecord>
         operations.size shouldBe 3
         operations[0].schema shouldBe Avro.default.schema(Operation.Nullary.serializer())
         operations[1].schema shouldBe Avro.default.schema(Operation.Unary.Negate.serializer())
         operations[2].schema shouldBe Avro.default.schema(Operation.Binary.Add.serializer())

         operations[1]["value"] shouldBe 1
         operations[2]["left"] shouldBe 3
         operations[2]["right"] shouldBe 4
      }
   }

   "read / write nullable sealed class" {

      @Serializable
      data class SealedClassTest(val a: Operation?)

      writeRead(SealedClassTest(null), SealedClassTest.serializer())
      writeRead(SealedClassTest(Operation.Nullary), SealedClassTest.serializer())
      writeRead(SealedClassTest(Operation.Unary.Negate(1)), SealedClassTest.serializer())
      writeRead(SealedClassTest(Operation.Unary.Negate(1)), SealedClassTest.serializer()) {
         val operation = it["a"] as GenericRecord
         operation.schema shouldBe Avro.default.schema(Operation.Unary.Negate.serializer())
         operation["value"] shouldBe 1
      }
   }

   "read / write sealed class directly"{
      writeRead(Operation.Nullary, Operation.serializer())
      writeRead(Operation.Unary.Negate(1), Operation.serializer())
      writeRead(Operation.Unary.Negate(1), Operation.serializer()) {operation ->
         operation.schema shouldBe Avro.default.schema(Operation.Unary.Negate.serializer())
         operation["value"] shouldBe 1
      }
   }
})