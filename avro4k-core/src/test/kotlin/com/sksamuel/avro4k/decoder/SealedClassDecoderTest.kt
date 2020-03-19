package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.schema.Operation.Binary
import com.sksamuel.avro4k.schema.ReferencingNullableSealedClass
import com.sksamuel.avro4k.schema.ReferencingSealedClass
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import org.apache.avro.generic.GenericData

class SealedClassDecoderTest : StringSpec({
   "support sealed classes" {
      val schema = Avro.default.schema(ReferencingSealedClass.serializer())
      val record = GenericData.Record(schema)
      val addSchema = Avro.default.schema(Binary.Add.serializer())
      val addRecord = GenericData.Record(addSchema)
      addRecord.put("left",1)
      addRecord.put("right",2)
      record.put("notNullable", addRecord)
      Avro.default.fromRecord(ReferencingSealedClass.serializer(), record) shouldBe ReferencingSealedClass(Binary.Add(1,2))
   }
   "support nullable sealed classes" {
      val schema = Avro.default.schema(ReferencingNullableSealedClass.serializer())
      val record = GenericData.Record(schema)
      val addSchema = Avro.default.schema(Binary.Add.serializer())
      val addRecord = GenericData.Record(addSchema)
      addRecord.put("left",1)
      addRecord.put("right",2)
      record.put("nullable", addRecord)
      Avro.default.fromRecord(ReferencingNullableSealedClass.serializer(), record) shouldBe ReferencingNullableSealedClass(Binary.Add(1,2))

      record.put("nullable",null)
      Avro.default.fromRecord(ReferencingNullableSealedClass.serializer(), record) shouldBe ReferencingNullableSealedClass(null)
   }
})