package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.io.AvroDecodeFormat
import com.github.avrokotlin.avro4k.io.AvroEncodeFormat
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
import kotlinx.serialization.Serializable
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.time.LocalDate

@Serializable
data class BugFoo(
    @Serializable(with = LocalDateSerializer::class)
    val d: LocalDate,
    val f: Float,
    val s: String
)

fun main() {
    val byteArrayOutputStream = ByteArrayOutputStream()
    val value = BugFoo(LocalDate.of(2002,1,1),2.3f,"34")
    val serializer = BugFoo.serializer()
    Avro.default.openOutputStream(serializer) {
        encodeFormat = AvroEncodeFormat.Binary
    }.to(byteArrayOutputStream).write(value).flush()

    val byteArrayInputStream = ByteArrayInputStream(byteArrayOutputStream.toByteArray())
    val result = Avro.default.openInputStream(serializer) {
        decodeFormat = AvroDecodeFormat.Binary(Avro.default.schema(serializer))
    }.from(byteArrayInputStream).next()

    println("Decoded from array stream: $result")
}