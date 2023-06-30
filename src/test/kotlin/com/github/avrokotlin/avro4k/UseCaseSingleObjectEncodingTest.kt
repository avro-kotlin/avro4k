package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.TestFixtures.fingerprint
import com.github.avrokotlin.avro4k.decoder.FooString
import com.github.avrokotlin.avro4k.decoder.FooStringJava
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.shouldNotBe
import org.apache.avro.Schema
import org.apache.avro.message.SchemaStore.Cache
import java.nio.ByteBuffer

/**
 * Use case test that takes [FooString] and [FooStringJava] and checks encoded bytes can be decoded/encoded and vice versa.
 */
internal class UseCaseSingleObjectEncodingTest : StringSpec({
   val str = "bar"

   val fooStringJava = FooStringJava.newBuilder().setStr(str).build()
   val fooStringKotlin = FooString(str)

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

   "encode fooStringKotlin and decode fooStringJava" {
      val bytes = TestFixtures.encodeSingleObject(FooString.serializer(), fooStringKotlin)

      val javaDecoded = FooStringJava.createDecoder(schemaStore).decode(bytes)

      javaDecoded shouldBeEqual fooStringJava
   }

   "encode fooStringKotlin and decode fooStringKotlin" {
      val bytes = TestFixtures.encodeSingleObject(FooString.serializer(), fooStringKotlin)

      val kotlinDecoded = TestFixtures.decodeSingleObject(
         bytes = bytes,
         serializer = FooString.serializer(),
         readerSchema = schemaFooStringKotlin,
         schemaStore = schemaStore
      )

      kotlinDecoded shouldBeEqual fooStringKotlin
   }

   "encode fooStringJava and decode fooStringKotlin" {
      val bytes = fooStringJava.toByteBuffer().array()

      val kotlinDecoded = TestFixtures.decodeSingleObject(
         bytes = bytes,
         serializer = FooString.serializer(),
         readerSchema = schemaFooStringKotlin,
         schemaStore = schemaStore
      )

      kotlinDecoded shouldBeEqual fooStringKotlin
   }

   "encode fooStringJava and decode fooStringJava" {
      val bytes = fooStringJava.toByteBuffer().array()
      val javaDecoded = FooStringJava.fromByteBuffer(ByteBuffer.wrap(bytes))

      javaDecoded shouldBeEqual fooStringJava
   }


})