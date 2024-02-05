package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decoder.FooString
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equals.shouldBeEqual
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericRecord
import org.apache.avro.message.SchemaStore
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class AvroSingleObjectInputStreamTest : StringSpec({

   "AvroSingleObjectInputStream should read otherFooString from fooString bytes" {
      val foo = FooString(str = "Bar")
      val writerSchema = Avro.default.schema(FooString.serializer())
      val schemaStore = SchemaStore.Cache().apply {
         addSchema(writerSchema)
      }
      val readerSchema = Avro.default.schema(OtherFooString.serializer())

      val baos = ByteArrayOutputStream()

      Avro.default.openOutputStream(FooString.serializer()) {
         this.encodeFormat = AvroEncodeFormat.SingleObject
         this.schema = writerSchema
      }.to(baos).write(foo).close()

      val decodedBytes = baos.toByteArray()

      val other: OtherFooString = Avro.default.openInputStream(OtherFooString.serializer()) {
         decodeFormat = AvroDecodeFormat.SingleObject(
            readerSchema = readerSchema,
            schemaStore = schemaStore
         )
      }.from(ByteArrayInputStream(decodedBytes)).nextOrThrow()

      // value is equal
      other.str.shouldBeEqual(foo.str)
   }

}) {


   /**
    * Another FooString data class with compatible Schema but different namespace/name (and thus: fingerprint)
    */
   @Serializable
   data class OtherFooString(val str: String)

}
