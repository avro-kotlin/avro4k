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

   /**
    * This is the avro single object encoded hex representation of `FooString(str="bar")`.
    *
    * C3 01 - avro marker bytes
    * CD 1D 19 01 C3 39 C6 61 - encoded writer schema fingerprint for lookup.
    * 06 62 61 72 - payload: 06: 3-letters in HEX, b=62,a=61,r=72
    *
    */
   const val FOO_STRING_SINGLE_OBJECT_HEX = "[C3 01 CD 1D 19 01 C3 39 C6 61 06 62 61 72]"

   fun ByteArray.toHexList() = toHexString(prefix = "", suffix = "", separator = ",").split(",")

   fun ByteArray.toHexString(separator: String = " ", prefix: String = "[", suffix: String = "]"): String =
      this.joinToString(
         separator = separator,
         prefix = prefix,
         postfix = suffix
      ) { "%02X".format(it) }

   fun readBytes(hexString: String, separator: String = " ", prefix: String = "[", suffix: String = "]"): ByteArray =
      hexString
         .removePrefix(prefix)
         .removeSuffix(suffix).split(separator)
         .map { Integer.valueOf(it, 16) }
         .map { it.toByte() }.toByteArray()

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
