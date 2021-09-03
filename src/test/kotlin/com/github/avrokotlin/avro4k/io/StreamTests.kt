package com.github.avrokotlin.avro4k.io


import com.github.avrokotlin.avro4k.Avro
import io.kotest.matchers.shouldBe
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import java.io.ByteArrayOutputStream

fun <T> writeRead(t: T, serializer: KSerializer<T>, avro: Avro = Avro.default) {
   writeData(t, serializer, avro).apply {
      val record = readData(this, serializer, avro)
      val tt = avro.fromRecord(serializer, record)
      t shouldBe tt
   }
   writeBinary(t, serializer, avro).apply {
      val record = readBinary(this, serializer, avro)
      val tt = avro.fromRecord(serializer, record)
      t shouldBe tt
   }
   writeJson(t, serializer, avro).apply {
      val record = readJson(this, serializer, avro)
      val tt = avro.fromRecord(serializer, record)
      t shouldBe tt
   }
}

fun <T> writeRead(t: T, expected: T, serializer: KSerializer<T>, avro: Avro = Avro.default) {
   writeData(t, serializer, avro).apply {
      val record = readData(this, serializer, avro)
      val tt = avro.fromRecord(serializer, record)
      tt shouldBe expected
   }
   writeBinary(t, serializer, avro).apply {
      val record = readBinary(this, serializer, avro)
      val tt = avro.fromRecord(serializer, record)
      tt shouldBe expected
   }
}

fun <T> writeRead(t: T, serializer: KSerializer<T>, avro: Avro = Avro.default, test: (GenericRecord) -> Any) {
   writeData(t, serializer, avro).apply {
      val record = readData(this, serializer, avro)
      test(record)
   }
   writeBinary(t, serializer, avro).apply {
      val record = readBinary(this, serializer, avro)
      test(record)
   }
   writeJson(t, serializer, avro).apply {
      val record = readJson(this, serializer, avro)
      test(record)
   }
}

fun <T> writeData(t: T, serializer: SerializationStrategy<T>, avro: Avro = Avro.default): ByteArray {
   val schema = avro.schema(serializer)
   val out = ByteArrayOutputStream()
   val output = avro.openOutputStream(serializer) {
      encodeFormat = AvroEncodeFormat.Data()
      this.schema = schema
   }.to(out)
   output.write(t)
   output.close()
   return out.toByteArray()
}

fun <T> readJson(bytes: ByteArray, serializer: KSerializer<T>, avro: Avro = Avro.default): GenericRecord {
   val schema = avro.schema(serializer)
   val datumReader = GenericDatumReader<GenericRecord>(schema)
   val decoder = DecoderFactory.get().jsonDecoder(schema, SeekableByteArrayInput(bytes))
   return datumReader.read(null, decoder)
}

fun <T> writeJson(t: T, serializer: KSerializer<T>, avro: Avro = Avro.default): ByteArray {
   val schema = avro.schema(serializer)
   val baos = ByteArrayOutputStream()
   val output = avro.openOutputStream(serializer) {
      encodeFormat = AvroEncodeFormat.Json
      this.schema = schema
   }.to(baos)
   output.write(t)
   output.close()
   return baos.toByteArray()
}

fun <T> readData(bytes: ByteArray, serializer: KSerializer<T>, avro: Avro = Avro.default): GenericRecord {
   val schema = avro.schema(serializer)
   val input = avro.openInputStream {
      decodeFormat = AvroDecodeFormat.Data(schema)
   }.from(bytes)
   return input.next() as GenericRecord
}

fun <T> writeBinary(t: T, serializer: SerializationStrategy<T>, avro: Avro = Avro.default): ByteArray {
   val schema = avro.schema(serializer)
   val out = ByteArrayOutputStream()
   val output = avro.openOutputStream(serializer) {
      encodeFormat = AvroEncodeFormat.Binary
      this.schema = schema
   }.to(out)
   output.write(t)
   output.close()
   return out.toByteArray()
}

fun <T> readBinary(bytes: ByteArray, serializer: KSerializer<T>, avro: Avro = Avro.default): GenericRecord {
   val schema = avro.schema(serializer)
   val datumReader = GenericDatumReader<GenericRecord>(schema)
   val decoder = DecoderFactory.get().binaryDecoder(SeekableByteArrayInput(bytes), null)
   return datumReader.read(null, decoder)
}