package com.sksamuel.avro4k.io

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroName
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import java.io.File

class AvroNameIoTest : StringSpec({

   "using @AvroName to write out a record" {

      @Serializable
      data class Composer(@AvroName("fullname") val name: String, val status: String)

      val ennio = Composer("Ennio Morricone", "Maestro")

      // writing out using the schema derived from Compose means fullname should be used
      val bytes = Avro.default.encodeToByteArray(Composer.serializer(), ennio)

      // using a custom schema to check that fullname was definitely used
      val schema = SchemaBuilder.record("Composer").fields()
         .name("fullname").type(Schema.create(Schema.Type.STRING)).noDefault()
         .name("status").type(Schema.create(Schema.Type.STRING)).noDefault()
         .endRecord()

      AvroInputStream.data(Composer.serializer(), schema).from(bytes).nextOrThrow() shouldBe ennio

   }

   "using @AvroName to read a record back in" {

      val schema1 = SchemaBuilder.record("Composer").fields()
         .name("fullname").type(Schema.create(Schema.Type.STRING)).noDefault()
         .name("status").type(Schema.create(Schema.Type.STRING)).noDefault()
         .endRecord()

      val record = GenericData.Record(schema1)
      record.put("fullname", "Ennio Morricone")
      record.put("status", "Maestro")

      val file = File("avroname.avro")

      val output = AvroOutputStream.binary(schema1).to(file)
      output.write(record)
      output.close()

      @Serializable
      data class Composer(@AvroName("fullname") val name: String, val status: String)

      val schema2 = Avro.default.schema(Composer.serializer())
      val input = AvroInputStream.binary(Composer.serializer(), schema2).from(file)

      input.next() shouldBe Composer("Ennio Morricone", "Maestro")
      input.close()

      file.delete()
   }

})