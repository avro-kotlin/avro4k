package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.Operation
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericRecord

class SealedClassIoTest : StringSpec({

    "read / write sealed class" {

        writeRead(SealedClassTest(Operation.Unary.Negate(1)), SealedClassTest.serializer())
        writeRead(SealedClassTest(Operation.Unary.Negate(1)), SealedClassTest.serializer()) {
            val operation = it["a"] as GenericRecord
            operation.schema shouldBe Avro.default.schema(Operation.Unary.Negate.serializer())
            operation["value"] shouldBe 1
        }
    }

    "read / write sealed class using object" {

        writeRead(SealedClassTest(Operation.Nullary), SealedClassTest.serializer())
        writeRead(SealedClassTest(Operation.Nullary), SealedClassTest.serializer()) {
            val operation = it["a"] as GenericRecord
            operation.schema shouldBe Avro.default.schema(Operation.Nullary.serializer())
        }
    }

    "read / write list of sealed class values" {

        val test =
            SealedClassListTest(
                listOf(
                    Operation.Nullary,
                    Operation.Unary.Negate(1),
                    Operation.Binary.Add(3, 4)
                )
            )
        writeRead(test, SealedClassListTest.serializer())
        writeRead(test, SealedClassListTest.serializer()) {
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

        writeRead(NullableSealedClassTest(null), NullableSealedClassTest.serializer())
        writeRead(NullableSealedClassTest(Operation.Nullary), NullableSealedClassTest.serializer())
        writeRead(NullableSealedClassTest(Operation.Unary.Negate(1)), NullableSealedClassTest.serializer())
        writeRead(NullableSealedClassTest(Operation.Unary.Negate(1)), NullableSealedClassTest.serializer()) {
            val operation = it["a"] as GenericRecord
            operation.schema shouldBe Avro.default.schema(Operation.Unary.Negate.serializer())
            operation["value"] shouldBe 1
        }
    }

    "read / write sealed class directly" {
        writeRead(Operation.Nullary, Operation.serializer())
        writeRead(Operation.Unary.Negate(1), Operation.serializer())
        writeRead(Operation.Unary.Negate(1), Operation.serializer()) { operation ->
            operation.schema shouldBe Avro.default.schema(Operation.Unary.Negate.serializer())
            operation["value"] shouldBe 1
        }
    }
}) {
    @Serializable
    data class SealedClassTest(val a: Operation)

    @Serializable
    data class SealedClassListTest(val a: List<Operation>)

    @Serializable
    data class NullableSealedClassTest(val a: Operation?)
}