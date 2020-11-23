package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.StringSpec
import io.kotest.inspectors.forNone
import io.kotest.matchers.string.shouldContain
import kotlinx.serialization.Serializable
import java.io.ByteArrayOutputStream

class AvroBinaryOutputStreamTest : StringSpec({

   @Serializable
   data class Work(val name: String, val year: Int)

   @Serializable
   data class Composer(val name: String, val birthplace: String, val works: List<Work>)

   val ennio = Composer("ennio morricone", "rome", listOf(Work("legend of 1900", 1986), Work("ecstasy of gold", 1969)))

   val hans = Composer("hans zimmer", "frankfurt", listOf(Work("batman begins", 2007), Work("dunkirk", 2017)))

   "AvroBinaryOutputStream should not write schemas"  {

      val baos = ByteArrayOutputStream()
      Avro.default.openOutputStream(Composer.serializer()) {
         encodeFormat = AvroEncodeFormat.Binary
      }.to(baos).write(ennio).write(hans).close()

      // the schema should not be written in a binary stream
      listOf("name", "birthplace", "works", "year").forNone {
         String(baos.toByteArray()).shouldContain(it)
      }
   }

})