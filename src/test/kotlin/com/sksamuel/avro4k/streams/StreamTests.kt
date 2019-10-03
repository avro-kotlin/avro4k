package com.sksamuel.avro4k.streams

import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import java.io.ByteArrayOutputStream

fun <T> writeRead(t: T, serializer: KSerializer<T>) {
   writeData(t, serializer).apply {
      val record = readData(this, serializer)
      val tt = Avro.default.fromRecord(serializer, record)
      t shouldBe tt
   }
   writeBinary(t, serializer).apply {
      val record = readBinary(this, serializer)
      val tt = Avro.default.fromRecord(serializer, record)
      t shouldBe tt
   }
   writeJson(t, serializer).apply {
      println(String(this))
      val record = readJson(this, serializer)
      val tt = Avro.default.fromRecord(serializer, record)
      t shouldBe tt
   }
}

fun <T> writeRead(t: T, expected: T, serializer: KSerializer<T>) {
   writeData(t, serializer).apply {
      val record = readData(this, serializer)
      val tt = Avro.default.fromRecord(serializer, record)
      tt shouldBe expected
   }
   writeBinary(t, serializer).apply {
      val record = readBinary(this, serializer)
      val tt = Avro.default.fromRecord(serializer, record)
      tt shouldBe expected
   }
}

fun <T> writeRead(t: T, serializer: KSerializer<T>, test: (GenericRecord) -> Any) {
   writeData(t, serializer).apply {
      val record = readData(this, serializer)
      test(record)
   }
   writeBinary(t, serializer).apply {
      val record = readBinary(this, serializer)
      test(record)
   }
   writeJson(t, serializer).apply {
      val record = readJson(this, serializer)
      test(record)
   }
}

fun <T> writeData(t: T, serializer: SerializationStrategy<T>): ByteArray {
   val schema = Avro.default.schema(serializer)
   val out = ByteArrayOutputStream()
   val avro = AvroOutputStream.data(schema, serializer).to(out)
   avro.write(t)
   avro.close()
   return out.toByteArray()
}

fun <T> readJson(bytes: ByteArray, serializer: KSerializer<T>): GenericRecord {
   val schema = Avro.default.schema(serializer)
   val datumReader = GenericDatumReader<GenericRecord>(schema)
   val decoder = DecoderFactory.get().jsonDecoder(schema, SeekableByteArrayInput(bytes))
   return datumReader.read(null, decoder)
}


fun <T> writeJson(t: T, serializer: KSerializer<T>): ByteArray {
   val schema = Avro.default.schema(serializer)
   val baos = ByteArrayOutputStream()
   val output = AvroOutputStream.json(schema, serializer).to(baos)
   output.write(t)
   output.close()
   return baos.toByteArray()
}

fun <T> readData(bytes: ByteArray, serializer: KSerializer<T>): GenericRecord {
   val schema = Avro.default.schema(serializer)
   val datumReader = GenericDatumReader<GenericRecord>(schema)
   val dataFileReader = DataFileReader<GenericRecord>(SeekableByteArrayInput(bytes), datumReader)
   return dataFileReader.next()
}

fun <T> writeBinary(t: T, serializer: SerializationStrategy<T>): ByteArray {
   val schema = Avro.default.schema(serializer)
   val out = ByteArrayOutputStream()
   val avro = AvroOutputStream.binary(schema, serializer).to(out)
   avro.write(t)
   avro.close()
   return out.toByteArray()
}

fun <T> readBinary(bytes: ByteArray, serializer: KSerializer<T>): GenericRecord {
   val schema = Avro.default.schema(serializer)
   val datumReader = GenericDatumReader<GenericRecord>(schema)
   val decoder = DecoderFactory.get().binaryDecoder(SeekableByteArrayInput(bytes), null)
   return datumReader.read(null, decoder)
}