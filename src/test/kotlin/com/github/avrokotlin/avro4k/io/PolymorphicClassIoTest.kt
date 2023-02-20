package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.*
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

class PolymorphicClassIoTest : StringSpec({
    "read / write nested polymorphic class" {
        val avro = Avro(serializersModule = polymorphicModule)
        writeRead(ReferencingPolymorphicRoot(UnsealedChildOne("one")), ReferencingPolymorphicRoot.serializer(), avro)
        writeRead(ReferencingPolymorphicRoot(UnsealedChildOne("one")), ReferencingPolymorphicRoot.serializer(), avro) {
            val root = it["root"] as GenericRecord
            root.schema shouldBe avro.schema(UnsealedChildOne.serializer())
        }
    }
    "read / write nested polymorphic list" {
        val avro = Avro(serializersModule = polymorphicModule)
        writeRead(PolymorphicRootInList(listOf(UnsealedChildOne("one"))), PolymorphicRootInList.serializer(), avro)
        writeRead(PolymorphicRootInList(listOf(UnsealedChildOne("one"))), PolymorphicRootInList.serializer(), avro) {
            it["listOfRoot"].shouldBeInstanceOf<List<GenericRecord>>()
            val unsealeadChild = (it["listOfRoot"] as List<*>)[0] as GenericRecord
            unsealeadChild.schema shouldBe avro.schema(UnsealedChildOne.serializer())
        }
    }
    "read / write nested polymorphic map" {
        val avro = Avro(serializersModule = polymorphicModule)
        writeRead(PolymorphicRootInMap(mapOf("a" to UnsealedChildOne("one"))), PolymorphicRootInMap.serializer(), avro)
        writeRead(
            PolymorphicRootInMap(mapOf("a" to UnsealedChildOne("one"))),
            PolymorphicRootInMap.serializer(),
            avro
        ) {
            it["mapOfRoot"].shouldBeInstanceOf<Map<Utf8, GenericRecord>>()
            val unsealeadChild = (it["mapOfRoot"] as Map<*, *>)[Utf8("a")] as GenericRecord
            unsealeadChild.schema shouldBe avro.schema(UnsealedChildOne.serializer())
        }
    }
})