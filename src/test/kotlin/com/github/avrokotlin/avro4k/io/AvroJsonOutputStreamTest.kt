package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.StringSpec
import io.kotest.inspectors.forAll
import io.kotest.matchers.string.shouldContain
import kotlinx.serialization.Serializable
import java.io.ByteArrayOutputStream

class AvroJsonOutputStreamTest : StringSpec({

    @Serializable
    data class Work(val name: String, val year: Int)

    @Serializable
    data class Composer(val name: String, val birthplace: String, val works: List<Work>)

    val ennio = Composer("ennio morricone", "rome", listOf(Work("legend of 1900", 1986), Work("ecstasy of gold", 1969)))

    val hans = Composer("hans zimmer", "frankfurt", listOf(Work("batman begins", 2007), Work("dunkirk", 2017)))

    "AvroJsonOutputStream should write schemas"  {

        val baos = ByteArrayOutputStream()
        Avro.default.openOutputStream(Composer.serializer()) {
            encodeFormat = AvroEncodeFormat.Json
        }.to(baos).write(ennio).write(hans).close()
        val jsonString = String(baos.toByteArray())
        // the schema should be written in a json stream
        listOf("name", "birthplace", "works", "year").forAll {
            jsonString.shouldContain(it)
        }
    }

})