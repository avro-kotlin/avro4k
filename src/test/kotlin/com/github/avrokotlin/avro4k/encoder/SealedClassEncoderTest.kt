package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.schema.Operation
import com.github.avrokotlin.avro4k.schema.ReferencingNullableSealedClass
import com.github.avrokotlin.avro4k.schema.ReferencingSealedClass
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.StringSpec
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
      Avro.default.encode(
              ReferencingSealedClass.serializer(),
              ReferencingSealedClass(Operation.Binary.Add(1, 2))
      ) shouldBeContentOf ListRecord(schema, ListRecord(addSchema, 1, 2))
   }
   "support nullable sealed classes" {
      val schema = Avro.default.schema(ReferencingNullableSealedClass.serializer())
      val addSchema = Avro.default.schema(Operation.Binary.Add.serializer())

      Avro.default.encode(
              ReferencingNullableSealedClass.serializer(), ReferencingNullableSealedClass(
              Operation.Binary.Add(1, 2)
      )
      ) shouldBeContentOf ListRecord(schema, ListRecord(addSchema, 1, 2))


      Avro.default.encode(
              ReferencingNullableSealedClass.serializer(), ReferencingNullableSealedClass(
              null
      )
      ) shouldBeContentOf ListRecord(schema, null)
   }
})