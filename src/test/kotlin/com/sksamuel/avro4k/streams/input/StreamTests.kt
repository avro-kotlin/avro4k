package com.sksamuel.avro4k.streams.input

import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import kotlinx.serialization.SerializationStrategy
import java.io.ByteArrayOutputStream

fun <T> writeRead(t: T, serializer: SerializationStrategy<T>) {
   val a = writeData(t, serializer)
   readData(a, serializer) shouldBe t
   val b = writeBinary(t, serializer)
   readBinary(b, serializer) shouldBe t
}

fun <T> writeRead(t: T, expected: T, serializer: SerializationStrategy<T>) {
   val a = writeData(t, serializer)
   readData(a, serializer) shouldBe expected
   val b = writeBinary(t, serializer)
   readBinary(b, serializer) shouldBe expected
}

fun <T> writeData(t: T, serializer: SerializationStrategy<T>): ByteArrayOutputStream {
   val schema = Avro.default.schema(serializer)
   val out = ByteArrayOutputStream()
   // val avro = AvroOutputStream.data[T].to(out).build(schema)
   //avro.write(t)
   //avro.close()
   return out
}

fun <T> readData(out: ByteArrayOutputStream, serializer: SerializationStrategy<T>): T =
   readData(out.toByteArray(), serializer)

fun <T> readData(bytes: ByteArray, serializer: SerializationStrategy<T>): T {
   // AvroInputStream.data.from(bytes).build(implicitly[SchemaFor[T]].schema(DefaultFieldMapper)).iterator.next()
   TODO()
}

fun <T> writeBinary(t: T, serializer: SerializationStrategy<T>): ByteArrayOutputStream {
   val schema = Avro.default.schema(serializer)
   val out = ByteArrayOutputStream()
   //val avro = AvroOutputStream.binary[T].to(out).build(schema)
   //avro.write(t)
   //avro.close()
   return out
}

fun <T> readBinary(baos: ByteArrayOutputStream, serializer: SerializationStrategy<T>): T =
   readBinary(baos.toByteArray(), serializer)

fun <T> readBinary(bytes: ByteArray, serializer: SerializationStrategy<T>): T {
  // AvroInputStream.binary.from(bytes).build(implicitly[SchemaFor[T]].schema(DefaultFieldMapper)).iterator.next()
   TODO()
}
