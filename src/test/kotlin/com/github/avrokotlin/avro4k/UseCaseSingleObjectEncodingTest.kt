package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.TestFixtures.FOO_STRING_SINGLE_OBJECT_HEX
import com.github.avrokotlin.avro4k.TestFixtures.decodeSingleObject
import com.github.avrokotlin.avro4k.TestFixtures.encodeSingleObject
import com.github.avrokotlin.avro4k.TestFixtures.fingerprint
import com.github.avrokotlin.avro4k.TestFixtures.readBytes
import com.github.avrokotlin.avro4k.TestFixtures.toHexList
import com.github.avrokotlin.avro4k.decoder.FooString
import com.github.avrokotlin.avro4k.decoder.FooStringJava
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.shouldNotBe
import org.apache.avro.Schema
import org.apache.avro.message.SchemaStore.Cache

/**
 * Use case test that takes [FooString] and [FooStringJava] and checks encoded bytes can be decoded/encoded and vice versa.
 */
internal class UseCaseSingleObjectEncodingTest : StringSpec({
   val str = "bar"

   val schemaFooStringKotlin = Avro.default.schema(FooString.serializer())
   val schemaFooStringJava = FooStringJava.`SCHEMA$`

   val fingerprintFooStringJava = schemaFooStringJava.fingerprint()
   val fingerprintFooStringKotlin = schemaFooStringKotlin.fingerprint()

   // register both
   val schemaStore = Cache().apply {
      addSchema(schemaFooStringKotlin)
      addSchema(schemaFooStringJava)
   }

   "schema fingerprint for java and kotlin is the same if schema.name is the same" {
      // fingerprints differ because name is different
      fingerprintFooStringKotlin shouldNotBe fingerprintFooStringJava

      // but if we rename the included java schema the fingerprints are identical, meaning 100% schema compatibility
      val renamedSchemaJava =
         FooStringJava.`SCHEMA$`.toString(false).replace("FooStringJava", "FooString").let { Schema.Parser().parse(it) }

      renamedSchemaJava.fingerprint() shouldBeExactly fingerprintFooStringKotlin
   }

   "encoding kotlin to bytes is equal to expected hex" {
      val kotlinEncoded = encodeSingleObject(FooString.serializer(), FooString("bar"))

      // we used a different writer schema for encoding, so the HEX String differs from the java String.
      // we have to ignore the schema-fingerprint bytes for comparison
      val encodedHexValues = kotlinEncoded.toHexList()
      val existingHexValues = readBytes(FOO_STRING_SINGLE_OBJECT_HEX).toHexList()

      // first two marker bytes
      encodedHexValues.subList(0, 1) shouldBeEqual existingHexValues.subList(0, 1)

      // 4 actual payload bytes for "bar"
      encodedHexValues.subList(10, 13) shouldBeEqual existingHexValues.subList(10, 13)
   }

   "decoding from hex to kotlin contains correct value" {
      val singleObjectBytes = readBytes(FOO_STRING_SINGLE_OBJECT_HEX)
      val decoded = decodeSingleObject(singleObjectBytes, FooString.serializer(), schemaFooStringKotlin, schemaStore)

      decoded shouldBeEqual FooString("bar")
   }

   "encode with kotlin and decode with java" {
      val kotlinEncoded = encodeSingleObject(FooString.serializer(), FooString("bar"))

      val javaDecoded = FooStringJava.createDecoder(schemaStore).decode(kotlinEncoded)

      javaDecoded.str shouldBeEqual "bar"
   }
})