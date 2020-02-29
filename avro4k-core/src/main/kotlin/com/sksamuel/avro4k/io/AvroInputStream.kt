package com.sksamuel.avro4k.io

import com.sksamuel.avro4k.Avro
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
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
       * Creates an [AvroInputStreamBuilder] that will read instances of T
       * from binary encoded files.
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
      @Deprecated("Create an instance of [AvroInputStream] using the openInputStream method on Avro.default")
      fun <T> binary(serializer: KSerializer<T>,
                     writerSchema: Schema? = null,
                     avro: Avro = Avro.default): AvroInputStreamBuilder<T> {
         val writer = writerSchema ?: avro.schema(serializer)
         val converter = { record: Any -> avro.fromRecord(serializer, record as GenericRecord) }
         return AvroInputStreamBuilder(converter, AvroFormat.BinaryFormat, writer, null)
      }

      @Deprecated("Create an instance of [AvroInputStream] using the openInputStream method on Avro.default")
      fun binary(writerSchema: Schema): AvroInputStreamBuilder<Any> {
         return AvroInputStreamBuilder({ it }, AvroFormat.BinaryFormat, writerSchema, null)
      }

      /**
       * Creates an [AvroInputStreamBuilder] that will read instances of T from binary
       * encoded files with the schema present. To convert to instance of T, the
       * data source must contain [GenericRecord]s.
       */
      @Deprecated("Create an instance of [AvroInputStream] using the openInputStream method on Avro.default")
      fun <T> data(serializer: DeserializationStrategy<T>,
                   writerSchema: Schema? = null,
                   avro: Avro = Avro.default): AvroInputStreamBuilder<T> {
         val converter = { record: Any -> avro.fromRecord(serializer, record as GenericRecord) }
         return AvroInputStreamBuilder(converter, AvroFormat.DataFormat, writerSchema, null)
      }

      /**
       * Creates an [AvroInputStreamBuilder] that will read Avro values from binary
       * encoded files with the schema present.
       */
      @Deprecated("Create an instance of [AvroInputStream] using the openInputStream method on Avro.default")
      fun data(writerSchema: Schema? = null): AvroInputStreamBuilder<Any> =
         AvroInputStreamBuilder({ it }, AvroFormat.DataFormat, writerSchema, null)

      /**
       * Creates an [AvroInputStreamBuilder] that will read instances of T from json
       * encoded files. To convert to instance of T, the data source must contain [GenericRecord]s.
       *
       * JSON encoded files do not include the schema, and therefore
       * require a schema to make sense of the data. This is the writer schema, which is
       * the schema that was originally used when writing the data.
       *
       * This writer schema can be supplied here, or if omitted, then a schema
       * will be generated from the class using [Avro.schema]. This of course
       * requires that the types have not changed between writing the original
       * data and reading it back in here. Otherwise the schemas will not match.
       */
      @Deprecated("Create an instance of [AvroInputStream] using the openInputStream method on Avro")
      fun <T> json(serializer: KSerializer<T>,
                   writerSchema: Schema? = null,
                   avro: Avro = Avro.default): AvroInputStreamBuilder<T> {
         val wschema = writerSchema ?: avro.schema(serializer)
         val converter = { record: Any -> avro.fromRecord(serializer, record as GenericRecord) }
         return AvroInputStreamBuilder(converter, AvroFormat.JsonFormat, wschema, null)
      }

      /**
       * Creates an [AvroInputStreamBuilder] that will read Avro values from json
       * encoded files.
       */
      @Deprecated("Create an instance of [AvroInputStream] using the openInputStream method on Avro.default")
      fun json(writerSchema: Schema): AvroInputStreamBuilder<Any> =
         AvroInputStreamBuilder({ it }, AvroFormat.DataFormat, writerSchema, null)
   }
}

class AvroInputStreamBuilder<T>(private val converter: (Any) -> T,
                                private val format: AvroFormat,
                                private val wschema: Schema?,
                                private val rschema: Schema?) {

   fun withWriterSchema(schema: Schema) = AvroInputStreamBuilder(converter, format, schema, rschema)
   fun withReaderSchema(schema: Schema) = AvroInputStreamBuilder(converter, format, wschema, schema)

   fun from(path: Path): AvroInputStream<T> = from(Files.newInputStream(path))
   fun from(path: String): AvroInputStream<T> = from(Paths.get(path))
   fun from(file: File): AvroInputStream<T> = from(file.toPath())
   fun from(bytes: ByteArray): AvroInputStream<T> = from(ByteArrayInputStream(bytes))
   fun from(buffer: ByteBuffer): AvroInputStream<T> = from(ByteArrayInputStream(buffer.array()))

   fun from(source: InputStream): AvroInputStream<T> {
      return when (format) {
         AvroFormat.BinaryFormat -> AvroBinaryInputStream(source, converter, wschema!!, rschema)
         AvroFormat.JsonFormat -> AvroJsonInputStream(source, converter, wschema!!, rschema)
         AvroFormat.DataFormat -> when {
            wschema != null && rschema != null -> AvroDataInputStream(source, converter, wschema, rschema)
            wschema != null -> AvroDataInputStream(source, converter, wschema, wschema)
            else -> AvroDataInputStream(source, converter, null, null)
         }
      }
   }
}