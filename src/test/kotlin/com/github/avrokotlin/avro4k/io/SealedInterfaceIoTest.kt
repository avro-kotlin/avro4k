package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

class SealedInterfaceIoTest : StringSpec({

   "read / write sealed interface" {

      writeRead(SealedInterfaceTest(ClassImplSealedInterface("a")), SealedInterfaceTest.serializer())
      writeRead(SealedInterfaceTest(ClassImplSealedInterface("a")), SealedInterfaceTest.serializer()) {
         val operation = it["a"] as GenericRecord
         operation.schema shouldBe Avro.default.schema(ClassImplSealedInterface.serializer())
         operation["value"] shouldBe Utf8("a")
      }
   }

   "read / write sealed interface using object" {

      writeRead(SealedInterfaceTest(ObjectImplSealedInterface), SealedInterfaceTest.serializer())
      writeRead(SealedInterfaceTest(ObjectImplSealedInterface), SealedInterfaceTest.serializer()) {
         val operation = it["a"] as GenericRecord
         operation.schema shouldBe Avro.default.schema(ObjectImplSealedInterface.serializer())
      }
   }

   "read / write list of sealed interface values" {

      val test = SealedInterfaceListTest(listOf(
         ObjectImplSealedInterface,
         ClassImplSealedInterface("b")
      ))
      writeRead(test, SealedInterfaceListTest.serializer())
      writeRead(test, SealedInterfaceListTest.serializer()) {
         it["a"].shouldBeInstanceOf<List<GenericRecord>>()
         @Suppress("UNCHECKED_CAST")
         val operations = it["a"] as List<GenericRecord>
         operations.size shouldBe 2
         operations[0].schema shouldBe Avro.default.schema(ObjectImplSealedInterface.serializer())
         operations[1].schema shouldBe Avro.default.schema(ClassImplSealedInterface.serializer())

         operations[1]["value"] shouldBe Utf8("b")
      }
   }

   "read / write nullable sealed class" {
      writeRead(NullableSealedInterfaceTest(null), NullableSealedInterfaceTest.serializer())
      writeRead(NullableSealedInterfaceTest(ObjectImplSealedInterface), NullableSealedInterfaceTest.serializer())
      writeRead(NullableSealedInterfaceTest(ClassImplSealedInterface("blub")), NullableSealedInterfaceTest.serializer())
      writeRead(NullableSealedInterfaceTest(ClassImplSealedInterface("blub")), NullableSealedInterfaceTest.serializer()) {
         val operation = it["a"] as GenericRecord
         operation.schema shouldBe Avro.default.schema(ClassImplSealedInterface.serializer())
         operation["value"] shouldBe Utf8("blub")
      }
   }

   "read / write mixed hierarchy with sealed interfaces" {
      val module = SerializersModule {
         polymorphic(UnsealedInterface::class) {
            subclass(SealedInterface::class)
         }
      }
      val avro = Avro(serializersModule = module)
      val toWrite = MixedHierarchySealedInterfaceTest(ClassImplSealedInterface("ba"))
      writeRead(toWrite,  MixedHierarchySealedInterfaceTest.serializer(), avro)
   }
}) {

   interface UnsealedInterface

   sealed interface SealedInterface : UnsealedInterface
   
   @Serializable
   object ObjectImplSealedInterface : SealedInterface
   
   @Serializable
   data class ClassImplSealedInterface (val value : String) : SealedInterface

   @Serializable
   data class SealedInterfaceTest(val a: SealedInterface)

   @Serializable
   data class SealedInterfaceListTest(val a: List<SealedInterface>)

   @Serializable
   data class NullableSealedInterfaceTest(val a: SealedInterface?)
   
   @Serializable
   data class MixedHierarchySealedInterfaceTest(val a : UnsealedInterface)
}
