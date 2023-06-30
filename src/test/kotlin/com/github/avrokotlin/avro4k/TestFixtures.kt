package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.io.AvroDecodeFormat
import com.github.avrokotlin.avro4k.io.AvroEncodeFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import org.apache.avro.Schema
import org.apache.avro.SchemaNormalization
import org.apache.avro.message.SchemaStore
import java.io.ByteArrayOutputStream

object TestFixtures {

   fun ByteArray.toHexList() = toHexString(prefix = "", postfix = "", separator = ",").split(",")

   fun ByteArray.toHexString(separator: String = " ", prefix: String = "[", postfix: String = "]"): String =
      this.joinToString(
         separator = separator,
         prefix = prefix,
         postfix = postfix
      ) { "%02X".format(it) }


   fun Schema.fingerprint() = SchemaNormalization.parsingFingerprint64(this)

   fun <T> encodeSingleObject(serializer: SerializationStrategy<T>, instance: T): ByteArray {
      val baos = ByteArrayOutputStream()
      Avro.default.openOutputStream(serializer) {
         encodeFormat = AvroEncodeFormat.SingleObject
      }.to(baos).write(instance).close()
      return baos.toByteArray()
   }

   fun <T> decodeSingleObject(
      bytes: ByteArray,
      serializer: DeserializationStrategy<T>,
      readerSchema: Schema,
      schemaStore: SchemaStore
   ): T {
      val input = Avro.default.openInputStream(serializer) {
         decodeFormat = AvroDecodeFormat.SingleObject(
            readerSchema = readerSchema,
            schemaStore = schemaStore
         )
      }.from(bytes)
      val kotlinDecoded = input.nextOrThrow()
      input.close()

      return kotlinDecoded
   }
}
