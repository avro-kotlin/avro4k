package com.sksamuel.avro4k.streams.input

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.streams.AvroInputStream
import io.kotlintest.shouldBe
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import java.io.ByteArrayOutputStream

fun <T : Any> writeRead(t: T, serializer: KSerializer<T>) {
   val a = writeData(t, serializer)
   readData(a, serializer) shouldBe t
   val b = writeBinary(t, serializer)
   readBinary(b, serializer) shouldBe t
}

fun <T : Any> writeRead(t: T, expected: T, serializer: KSerializer<T>) {
   val a = writeData(t, serializer)
   readData(a, serializer) shouldBe expected
   val b = writeBinary(t, serializer)
   readBinary(b, serializer) shouldBe expected
}

fun <T : Any> writeData(t: T, serializer: SerializationStrategy<T>): ByteArrayOutputStream {
   val schema = Avro.default.schema(serializer)
   val out = ByteArrayOutputStream()
   // val avro = AvroOutputStream.data[T].to(out).build(schema)
   //avro.write(t)
   //avro.close()
   return out
}

fun <T : Any> readData(out: ByteArrayOutputStream, deserializer: DeserializationStrategy<T>): T =
   readData(out.toByteArray(), deserializer)

fun <T : Any> readData(bytes: ByteArray, deserializer: DeserializationStrategy<T>): T {
   return AvroInputStream.data(deserializer).from(bytes).nextOrThrow()
}

fun <T : Any> writeBinary(t: T, serializer: SerializationStrategy<T>): ByteArrayOutputStream {
   val schema = Avro.default.schema(serializer)
   val out = ByteArrayOutputStream()
   //val avro = AvroOutputStream.binary[T].to(out).build(schema)
   //avro.write(t)
   //avro.close()
   return out
}

fun <T : Any> readBinary(baos: ByteArrayOutputStream, deserializer: DeserializationStrategy<T>): T =
   readBinary(baos.toByteArray(), deserializer)

fun <T : Any> readBinary(bytes: ByteArray, deserializer: DeserializationStrategy<T>): T {
   return AvroInputStream.binary(deserializer).from(bytes).nextOrThrow()
}
