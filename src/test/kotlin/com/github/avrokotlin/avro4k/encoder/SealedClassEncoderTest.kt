package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.Operation
import com.github.avrokotlin.avro4k.schema.ReferencingNullableSealedClass
import com.github.avrokotlin.avro4k.schema.ReferencingSealedClass
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.spec.style.stringSpec

fun sealedClassEncoderTests(encoderToTest: EncoderToTest) : TestFactory {
   return stringSpec {
      "support sealed classes" {
         val addSchema = encoderToTest.avro.schema(Operation.Binary.Add.serializer())
         encoderToTest.testEncodeDecode(ReferencingSealedClass(Operation.Binary.Add(1, 2)), record(record(1,2).createRecord(addSchema)))
      }
      "support nullable sealed classes" {
         val addSchema = Avro.default.schema(Operation.Binary.Add.serializer())

         encoderToTest.testEncodeDecode(ReferencingNullableSealedClass(
            Operation.Binary.Add(1, 2)
         ), record(record(1,2).createRecord(addSchema)))

         encoderToTest.testEncodeDecode(ReferencingNullableSealedClass(null), record(null))
      }
   }
}
class SealedClassEncoderTest : StringSpec({
   includeForEveryEncoder { sealedClassEncoderTests(it) }
})