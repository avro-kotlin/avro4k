package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.TestFixtures.toHexList
import com.github.avrokotlin.avro4k.decoder.FooString
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equals.shouldBeEqual
import org.apache.avro.SchemaNormalization
import java.io.ByteArrayOutputStream

class AvroSingleObjectOutputStreamTest : StringSpec({

   "AvroSingleObjectOutputStream should write fooString bytes" {
      val writerSchema = Avro.default.schema(FooString.serializer().descriptor)

      val baos = ByteArrayOutputStream()
      val foo = FooString(str = "Bar")

      // we are using the bytes as hex here for easier comparison
      val schemaHexList = SchemaNormalization.parsingFingerprint("CRC-64-AVRO", writerSchema).toHexList()

      // this reuses existing functionality for the pure message payload.
      val messageBinaryHex = foo.toAvroBinaryWithoutSchema().toHexList()

      val stream = AvroSingleObjectOutputStream<FooString>(
         output = baos,
         converter = { Avro.default.toRecord(FooString.serializer(), writerSchema, it) },
         writerSchema = writerSchema
      )
      stream.write(foo).close()

      val byteHexList: List<String> = baos.toByteArray().toHexList()

      val avroV1Header = byteHexList.subList(0, 2)
      val avroSchemaFingerprint = byteHexList.subList(2, 10)
      val avroMessageBinary = byteHexList.subList(10, byteHexList.size)

      avroV1Header.shouldBeEqual(listOf("C3", "01"))
      avroSchemaFingerprint.shouldBeEqual(schemaHexList)
      avroMessageBinary.shouldBeEqual(messageBinaryHex)
   }

})

fun FooString.toAvroBinaryWithoutSchema(): ByteArray {
   val baos = ByteArrayOutputStream()
   Avro.default.openOutputStream(FooString.serializer()) {
      encodeFormat = AvroEncodeFormat.Binary
   }.to(baos).write(this).close()
   return baos.toByteArray()
}

