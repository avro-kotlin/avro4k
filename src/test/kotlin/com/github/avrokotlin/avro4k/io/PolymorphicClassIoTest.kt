package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.ReferencingPolymorphicRoot
import com.github.avrokotlin.avro4k.schema.UnsealedChildOne
import com.github.avrokotlin.avro4k.schema.UnsealedChildTwo
import com.github.avrokotlin.avro4k.schema.UnsealedPolymorphicRoot
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.apache.avro.generic.GenericRecord

class PolymorphicClassIoTest : StringSpec({
   "read / write nested polymorphic class" {
      val module = SerializersModule {
         polymorphic(UnsealedPolymorphicRoot::class) {
            subclass(UnsealedChildOne::class)
            subclass(UnsealedChildTwo::class)
         }
      }
      val avro = Avro(serializersModule = module)
      writeRead(ReferencingPolymorphicRoot(UnsealedChildOne("one")), ReferencingPolymorphicRoot.serializer(), avro)
      writeRead(ReferencingPolymorphicRoot(UnsealedChildOne("one")), ReferencingPolymorphicRoot.serializer(), avro) {
         val root = it["root"] as GenericRecord
         root.schema shouldBe avro.schema(UnsealedChildOne.serializer())
      }
   }
})