package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import com.sksamuel.avro4k.schema.Operation
import com.sksamuel.avro4k.schema.ReferencingNullableSealedClass
import com.sksamuel.avro4k.schema.ReferencingSealedClass
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import org.apache.avro.generic.GenericData

class SealedClassEncoderTest : StringSpec({

   "support sealed classes" {
      val schema = Avro.default.schema(ReferencingSealedClass.serializer())
      val record = GenericData.Record(schema)
      val addSchema = Avro.default.schema(Operation.Binary.Add.serializer())
      val addRecord = GenericData.Record(addSchema)
      addRecord.put("left", 1)
      addRecord.put("right", 2)
      record.put("notNullable", addRecord)
      Avro.default.toRecord(
         ReferencingSealedClass.serializer(),
         ReferencingSealedClass(Operation.Binary.Add(1, 2))
      ) shouldBe ListRecord(schema,ListRecord(addSchema,1,2))
   }
   "support nullable sealed classes" {
      val schema = Avro.default.schema(ReferencingNullableSealedClass.serializer())
      val addSchema = Avro.default.schema(Operation.Binary.Add.serializer())

      Avro.default.toRecord(
         ReferencingNullableSealedClass.serializer(), ReferencingNullableSealedClass(
            Operation.Binary.Add(1, 2)
         )
      ) shouldBe ListRecord(schema,ListRecord(addSchema,1,2))


      Avro.default.toRecord(
         ReferencingNullableSealedClass.serializer(), ReferencingNullableSealedClass(
           null
         )
      ) shouldBe ListRecord(schema,null)
   }
})