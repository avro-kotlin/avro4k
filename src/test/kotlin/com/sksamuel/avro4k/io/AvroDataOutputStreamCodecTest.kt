package com.sksamuel.avro4k.io

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.should
import io.kotest.matchers.shouldNot
import io.kotest.matchers.string.contain
import kotlinx.serialization.Serializable
import org.apache.avro.file.CodecFactory
import java.io.ByteArrayOutputStream

class AvroDataOutputStreamCodecTest : StringSpec({

   @Serializable
   data class Composer(val name: String, val birthplace: String, val compositions: List<String>)

   val ennio = Composer("ennio morricone", "rome", listOf("legend of 1900", "ecstasy of gold"))

   "include schema"  {
      val baos = ByteArrayOutputStream()
      val output = AvroOutputStream.data(Composer.serializer()).to(baos)
      output.write(ennio)
      output.close()
      String(baos.toByteArray()) should contain("birthplace")
      String(baos.toByteArray()) should contain("compositions")
   }

   "include snappy coded in metadata when serialized with snappy"  {

      val baos = ByteArrayOutputStream()
      val output = AvroOutputStream.data(Composer.serializer())
         .withCodec(CodecFactory.snappyCodec())
         .to(baos)
      output.write(ennio)
      output.close()
      String(baos.toByteArray()) should contain("snappy")
      String(baos.toByteArray()) shouldNot contain("bzip2")
      String(baos.toByteArray()) shouldNot contain("deflate")
   }

   "include deflate coded in metadata when serialized with deflate"  {
      val baos = ByteArrayOutputStream()
      val output = AvroOutputStream.data(Composer.serializer())
         .withCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL))
         .to(baos)
      output.write(ennio)
      output.close()
      String(baos.toByteArray()) should contain("deflate")
      String(baos.toByteArray()) shouldNot contain("bzip2")
      String(baos.toByteArray()) shouldNot contain("snappy")
   }

   "include bzip2 coded in metadata when serialized with bzip2"  {
      val baos = ByteArrayOutputStream()
      val output = AvroOutputStream.data(Composer.serializer())
         .withCodec(CodecFactory.bzip2Codec())
         .to(baos)
      output.write(ennio)
      output.close()
      String(baos.toByteArray()) should contain("bzip2")
      String(baos.toByteArray()) shouldNot contain("deflate")
      String(baos.toByteArray()) shouldNot contain("snappy")
   }

})