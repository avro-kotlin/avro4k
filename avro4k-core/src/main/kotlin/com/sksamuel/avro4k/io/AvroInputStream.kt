package com.sksamuel.avro4k.io

import com.sksamuel.avro4k.Avro
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import org.apache.avro.Schema
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

interface AvroInputStream<T> : AutoCloseable {

   /**
    * Returns a [Sequence] for the values of T in the stream.
    * This function should not be invoked if using [next].
    */
   fun iterator(): Iterator<T> = iterator<T> {
      var next = next()
      while (next != null) {
         yield(next)
         next = next()
      }
   }

   /**
    * Returns the next value of T in the stream.
    * This function should not be invoked if using [iterator].
    */
   fun next(): T?

   fun nextOrThrow(): T = next() ?: throw SerializationException("No next entity found")

   companion object {

      /**
       * Creates an [AvroInputStreamBuilder] that will read from binary
       * encoded files.
       *
       * Binary encoded files do not include the schema, and therefore
       * require a schema to make sense of the data. This is known as the
       * 'writer schema' or in other words, the schema that was originally
       * used when creating the data.
       *
       * This writer schema can be supplied here, or if omitted, then a schema
       * will be generated from the class using [Avro.schema]. This of course
       * requires that the types have not changed between writing the original
       * data and reading it back in here. Otherwise the schemas will not match.
       */
      fun <T> binary(serializer: KSerializer<T>,
                     writerSchema: Schema? = null,
                     avro: Avro = Avro.default): AvroInputStreamBuilder<T> {
         val writer = writerSchema ?: avro.schema(serializer)
         return AvroInputStreamBuilder(serializer, AvroFormat.BinaryFormat, writer, null, avro)
      }

      /**
       * Creates an [AvroInputStreamBuilder] that will read from binary
       * encoded files with the schema present.
       */
      fun <T> data(serializer: DeserializationStrategy<T>,
                   writerSchema: Schema? = null,
                   avro: Avro = Avro.default): AvroInputStreamBuilder<T> =
         AvroInputStreamBuilder(serializer, AvroFormat.DataFormat, writerSchema, null, avro)

      /**
       * Creates an [AvroInputStreamBuilder] that will read from json
       * encoded files.
       *
       * JSON encoded files do not include the schema, and therefore
       * require a schema to make sense of the data. This is the writer schema, which is
       * the schema that was originally used when writing the data.
       */
      fun <T> json(serializer: KSerializer<T>,
                   writerSchema: Schema? = null,
                   avro: Avro = Avro.default): AvroInputStreamBuilder<T> {
         val writer = writerSchema ?: avro.schema(serializer)
         return AvroInputStreamBuilder(serializer, AvroFormat.JsonFormat, writer, null, avro)
      }
   }
}

class AvroInputStreamBuilder<T>(private val deserializer: DeserializationStrategy<T>,
                                private val format: AvroFormat,
                                private val wschema: Schema?,
                                private val rschema: Schema?,
                                private val avro: Avro) {

   fun withWriterSchema(schema: Schema) = AvroInputStreamBuilder(deserializer, format, schema, rschema, avro)
   fun withReaderSchema(schema: Schema) = AvroInputStreamBuilder(deserializer, format, wschema, schema, avro)

   fun from(path: Path): AvroInputStream<T> = from(Files.newInputStream(path))
   fun from(path: String): AvroInputStream<T> = from(Paths.get(path))
   fun from(file: File): AvroInputStream<T> = from(file.toPath())
   fun from(bytes: ByteArray): AvroInputStream<T> = from(ByteArrayInputStream(bytes))
   fun from(buffer: ByteBuffer): AvroInputStream<T> = from(ByteArrayInputStream(buffer.array()))

   fun from(source: InputStream): AvroInputStream<T> {
      return when (format) {
         AvroFormat.BinaryFormat -> AvroBinaryInputStream(source, deserializer, wschema!!, rschema, avro)
         AvroFormat.JsonFormat -> AvroJsonInputStream(source, deserializer, wschema!!, rschema, avro)
         AvroFormat.DataFormat -> when {
            wschema != null && rschema != null -> AvroDataInputStream(source, deserializer, wschema, rschema, avro)
            wschema != null -> AvroDataInputStream(source, deserializer, wschema, wschema, avro)
            else -> AvroDataInputStream(source, deserializer, null, null, avro)
         }
      }
   }
}