package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroName
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.nio.file.Files

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

      Avro.default.openInputStream(Composer.serializer()){
         decodeFormat = AvroDecodeFormat.Data(schema, defaultReadSchema)
      }.from(bytes).nextOrThrow() shouldBe ennio
   }

   "using @AvroName to read a record back in" {

      val schema1 = SchemaBuilder.record("Composer").fields()
         .name("fullname").type(Schema.create(Schema.Type.STRING)).noDefault()
         .name("status").type(Schema.create(Schema.Type.STRING)).noDefault()
         .endRecord()

      val record = GenericData.Record(schema1)
      record.put("fullname", "Ennio Morricone")
      record.put("status", "Maestro")

      val file = Files.createTempFile("avroname.avro","")

      val outputStream = Files.newOutputStream(file)
      AvroBinaryOutputStream<GenericRecord>(outputStream, {it}, schema1).write(record).close()

      @Serializable
      data class Composer(@AvroName("fullname") val name: String, val status: String)

      val schema2 = Avro.default.schema(Composer.serializer())
      val input = Avro.default.openInputStream(Composer.serializer()) {
         decodeFormat = AvroDecodeFormat.Binary(
            writerSchema = schema1,
            readerSchema = schema2
         )
      }.from(file)

      input.next() shouldBe Composer("Ennio Morricone", "Maestro")
      input.close()

      Files.delete(file)
   }

})