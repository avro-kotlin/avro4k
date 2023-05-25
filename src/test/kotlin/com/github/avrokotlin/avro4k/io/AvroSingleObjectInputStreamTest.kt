package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decoder.FooString
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equals.shouldBeEqual
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericRecord
import org.apache.avro.message.SchemaStore
import java.io.ByteArrayInputStream

class AvroSingleObjectInputStreamTest : StringSpec({

   "AvroSingleObjectInputStream should read otherFooString from fooString bytes" {
      val foo = FooString(str = "Bar")
      val writerSchema = Avro.default.schema(FooString.serializer())
      val schemaStore = SchemaStore { writerSchema }
      val readerSchema = Avro.default.schema(OtherFooString.serializer())

      val decodedBytes = Avro.default.encodeToSingleObject(serializer = FooString.serializer(), value = foo)

      val stream = AvroSingleObjectInputStream<OtherFooString>(
         input = ByteArrayInputStream(decodedBytes),
         converter = { Avro.default.fromRecord(OtherFooString.serializer(), it as GenericRecord) },
         schemaStore = schemaStore,
         readerSchema = readerSchema
      )

      val other = requireNotNull(stream.next()) { "this should not happen" }

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
