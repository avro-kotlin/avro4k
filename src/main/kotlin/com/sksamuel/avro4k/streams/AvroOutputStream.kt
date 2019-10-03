package com.sksamuel.avro4k.streams

import kotlinx.serialization.SerializationStrategy
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import java.io.File
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * An [AvroOutputStream] will write instances of T to an underlying
 * representation.
 *
 * There are three implementations of this stream
 *  - a Data stream,
 *  - a Binary stream
 *  - a Json stream
 *
 * See the methods on the companion object to create instances of each
 * of these types of stream.
 */
interface AvroOutputStream<T> : AutoCloseable {

   fun flush()
   fun fSync()
   fun write(t: T)
   fun write(ts: List<T>): Unit = ts.forEach(::write)

   companion object {

      /**
       * An [AvroOutputStream] that writes as binary without the schema. Use this when
       * you want the smallest messages possible at the cost of not having the schema available
       * in the messages for downstream clients.
       */
      fun <T> binary(schema: Schema, serializer: SerializationStrategy<T>) =
         AvroOutputStreamBuilder(schema, serializer, AvroFormat.BinaryFormat)

      /**
       * An [AvroOutputStream] that writes as JSON.
       * The schema is not included.
       */
      fun <T> json(schema: Schema, serializer: SerializationStrategy<T>) =
         AvroOutputStreamBuilder(schema, serializer, AvroFormat.JsonFormat)

      /**
       * An [AvroOutputStream] that writes as binary with the inclusion of the schema.
       */
      fun <T> data(schema: Schema, serializer: SerializationStrategy<T>) =
         AvroOutputStreamBuilder(schema, serializer, AvroFormat.DataFormat)
   }
}

class AvroOutputStreamBuilder<T>(private val schema: Schema,
                                 private val serializer: SerializationStrategy<T>,
                                 private val format: AvroFormat,
                                 private val codec: CodecFactory = CodecFactory.nullCodec()) {

   fun withCodec(codec: CodecFactory) = AvroOutputStreamBuilder(schema, serializer, format, codec)

   fun to(path: Path): AvroOutputStream<T> = to(Files.newOutputStream(path))
   fun to(path: String): AvroOutputStream<T> = to(Paths.get(path))
   fun to(file: File): AvroOutputStream<T> = to(file.toPath())

   fun to(output: OutputStream): AvroOutputStream<T> = when (format) {
      AvroFormat.DataFormat -> AvroDataOutputStream(output, serializer, schema, codec)
      AvroFormat.JsonFormat -> AvroBinaryOutputStream(output, serializer, schema)
      AvroFormat.BinaryFormat -> AvroBinaryOutputStream(output, serializer, schema)
   }
}