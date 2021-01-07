package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.SnakeCaseNamingStrategy
import io.kotest.core.spec.style.StringSpec
import io.kotest.engine.spec.tempfile
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.io.ByteArrayOutputStream
import java.nio.file.Files

class NamingStrategyIoTest : StringSpec({

   @Serializable
   data class Composer(val fullName: String, val status: String)

   "using snake_case namingStrategy to write out a record" {
      val ennio = Composer("Ennio Morricone", "Maestro")

      val baos = ByteArrayOutputStream()

      Avro.default.openOutputStream(Composer.serializer(), SnakeCaseNamingStrategy) {
         encodeFormat = AvroEncodeFormat.Data()
      }.to(baos).use {
         it.write(ennio)
      }

      val schema = SchemaBuilder.record("Composer").fields()
         .name("full_name").type(Schema.create(Schema.Type.STRING)).noDefault()
         .name("status").type(Schema.create(Schema.Type.STRING)).noDefault()
         .endRecord()

      Avro.default.openInputStream(Composer.serializer(), SnakeCaseNamingStrategy) {
         decodeFormat = AvroDecodeFormat.Data(schema, defaultReadSchema)
      }.from(baos.toByteArray()).nextOrThrow() shouldBe ennio
   }

   "using snake_case namingStrategy to read a record back in" {

      val schema1 = SchemaBuilder.record("Composer").fields()
         .name("full_name").type(Schema.create(Schema.Type.STRING)).noDefault()
         .name("status").type(Schema.create(Schema.Type.STRING)).noDefault()
         .endRecord()

      val record = GenericData.Record(schema1)
      record.put("full_name", "Ennio Morricone")
      record.put("status", "Maestro")

      val file = tempfile("avroname.avro", "")

      AvroBinaryOutputStream<GenericRecord>(Files.newOutputStream(file.toPath()), { it }, schema1).use {
         it.write(record)
      }

      val schema2 = Avro.default.schema(Composer.serializer(), SnakeCaseNamingStrategy)

      Avro.default.openInputStream(Composer.serializer(), SnakeCaseNamingStrategy) {
         decodeFormat = AvroDecodeFormat.Binary(
            writerSchema = schema1,
            readerSchema = schema2
         )
      }.from(file).use {
         it.next() shouldBe Composer("Ennio Morricone", "Maestro")
      }
   }
})
