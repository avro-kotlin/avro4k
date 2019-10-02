package com.sksamuel.avro4k.streams

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import org.apache.avro.Schema
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

interface AvroInputStream<T : Any> : AutoCloseable {

   /**
    * Returns a [Sequence] for the values of T in the stream.
    * This function should not be invoked if using [next].
    */
   fun seq(): Sequence<T> = generateSequence { next() }

   /**
    * Returns the next value of T in the stream.
    * This function should not be invoked if using [seq].
    */
   fun next(): T?

   fun nextOrThrow(): T = next() ?: throw SerializationException("No next entity found")

   companion object {

      /**
       * Creates an [AvroInputStreamBuilder] that will read from binary
       * encoded files.
       */
      fun <T : Any> binary(deserializer: DeserializationStrategy<T>): AvroInputStreamBuilder<T> =
         AvroInputStreamBuilder(deserializer, AvroFormat.BinaryFormat, null, null)

      /**
       * Creates an [AvroInputStreamBuilder] that will read from binary
       * encoded files with the schema present.
       */
      fun <T : Any> data(deserializer: DeserializationStrategy<T>): AvroInputStreamBuilder<T> =
         AvroInputStreamBuilder(deserializer, AvroFormat.DataFormat, null, null)

      /**
       * Creates an [AvroInputStreamBuilder] that will read from json
       * encoded files.
       */
      fun <T : Any> json(deserializer: DeserializationStrategy<T>): AvroInputStreamBuilder<T> =
         AvroInputStreamBuilder(deserializer, AvroFormat.JsonFormat, null, null)
   }
}

class AvroInputStreamBuilder<T : Any>(private val deserializer: DeserializationStrategy<T>,
                                      private val format: AvroFormat,
                                      private val readerSchema: Schema?,
                                      private val writerSchema: Schema?) {

   fun withWriterSchema(schema: Schema?) = AvroInputStreamBuilder(deserializer, format, readerSchema, schema)
   fun withReaderSchema(schema: Schema?) = AvroInputStreamBuilder(deserializer, format, schema, writerSchema)

   fun from(path: Path): AvroInputStream<T> = from(Files.newInputStream(path))
   fun from(path: String): AvroInputStream<T> = from(Paths.get(path))
   fun from(file: File): AvroInputStream<T> = from(file.toPath())
   fun from(bytes: ByteArray): AvroInputStream<T> = from(ByteArrayInputStream(bytes))
   fun from(buffer: ByteBuffer): AvroInputStream<T> = from(ByteArrayInputStream(buffer.array()))

   fun from(source: InputStream): AvroInputStream<T> {
      return when (format) {
         AvroFormat.BinaryFormat -> when {
            writerSchema != null && readerSchema != null ->
               AvroBinaryInputStream(source, deserializer, writerSchema, readerSchema)
            writerSchema != null ->
               AvroBinaryInputStream(source, deserializer, writerSchema, writerSchema)
            else ->
               throw SerializationException("Must specify a schema for binary formats")
         }
         AvroFormat.JsonFormat -> when {
            writerSchema != null && readerSchema != null ->
               AvroJsonInputStream(source, deserializer, writerSchema, readerSchema)
            writerSchema != null ->
               AvroJsonInputStream(source, deserializer, writerSchema, writerSchema)
            else ->
               throw SerializationException("Must specify a schema for json formats")
         }
         AvroFormat.DataFormat -> when {
            writerSchema != null && readerSchema != null ->
               AvroDataInputStream(source, deserializer, writerSchema, readerSchema)
            writerSchema != null ->
               AvroDataInputStream(source, deserializer, writerSchema, writerSchema)
            else ->
               AvroDataInputStream(source, deserializer, null, null)
         }
      }
   }
}