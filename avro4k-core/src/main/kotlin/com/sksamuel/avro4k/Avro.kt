package com.sksamuel.avro4k

import com.sksamuel.avro4k.decoder.RootRecordDecoder
import com.sksamuel.avro4k.encoder.RootRecordEncoder
import com.sksamuel.avro4k.io.AvroBinaryInputStream
import com.sksamuel.avro4k.io.AvroBinaryOutputStream
import com.sksamuel.avro4k.io.AvroDataInputStream
import com.sksamuel.avro4k.io.AvroDataOutputStream
import com.sksamuel.avro4k.io.AvroFormat
import com.sksamuel.avro4k.io.AvroInputStream
import com.sksamuel.avro4k.io.AvroJsonInputStream
import com.sksamuel.avro4k.io.AvroJsonOutputStream
import com.sksamuel.avro4k.io.AvroOutputStream
import com.sksamuel.avro4k.schema.DefaultNamingStrategy
import com.sksamuel.avro4k.schema.schemaFor
import com.sksamuel.avro4k.serializer.UUIDSerializer
import kotlinx.serialization.AbstractSerialFormat
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.encode
import kotlinx.serialization.modules.EmptyModule
import kotlinx.serialization.modules.SerialModule
import kotlinx.serialization.modules.serializersModuleOf
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

data class AvroConfiguration constructor(
   @JvmField internal val encodeDefaults: Boolean = true
)

class AvroInputStreamBuilder<T>(private val converter: (Any) -> T) {

   var format: AvroFormat = AvroFormat.DataFormat
   var writerSchema: Schema? = null
   var readerSchema: Schema? = null

   fun from(path: Path): AvroInputStream<T> = from(Files.newInputStream(path))
   fun from(path: String): AvroInputStream<T> = from(Paths.get(path))
   fun from(file: File): AvroInputStream<T> = from(file.toPath())
   fun from(bytes: ByteArray): AvroInputStream<T> = from(ByteArrayInputStream(bytes))
   fun from(buffer: ByteBuffer): AvroInputStream<T> = from(ByteArrayInputStream(buffer.array()))

   fun from(source: InputStream): AvroInputStream<T> {
      return when (format) {
         AvroFormat.BinaryFormat -> {
            val wschema = writerSchema
               ?: throw SerializationException("Cannot read from binary unless a writer schema is specified")
            AvroBinaryInputStream(source, converter, wschema, readerSchema)
         }
         AvroFormat.JsonFormat -> {
            val wschema = writerSchema
               ?: throw SerializationException("Cannot read from json unless a writer schema is specified")
            AvroJsonInputStream(source, converter, wschema, readerSchema)
         }
         AvroFormat.DataFormat -> when {
            writerSchema != null && readerSchema != null ->
               AvroDataInputStream(source, converter, writerSchema, readerSchema)
            writerSchema != null ->
               AvroDataInputStream(source, converter, writerSchema, readerSchema)
            else ->
               AvroDataInputStream(source, converter, null, null)
         }
      }
   }
}

class AvroOutputStreamBuilder<T>(private val serializer: SerializationStrategy<T>,
                                 private val avro: Avro,
                                 private val converterFn: (Schema) -> (T) -> GenericRecord) {

   var format: AvroFormat = AvroFormat.DataFormat
   var schema: Schema? = null
   var codec: CodecFactory = CodecFactory.nullCodec()

   fun to(path: Path): AvroOutputStream<T> = to(Files.newOutputStream(path))
   fun to(path: String): AvroOutputStream<T> = to(Paths.get(path))
   fun to(file: File): AvroOutputStream<T> = to(file.toPath())

   fun to(output: OutputStream): AvroOutputStream<T> {
      val schema = schema ?: avro.schema(serializer)
      val converter = converterFn(schema)
      return when (format) {
         AvroFormat.DataFormat -> AvroDataOutputStream(output, converter, schema, codec)
         AvroFormat.JsonFormat -> AvroJsonOutputStream(output, converter, schema)
         AvroFormat.BinaryFormat -> AvroBinaryOutputStream(output, converter, schema)
      }
   }
}

class Avro(override val context: SerialModule = EmptyModule) : AbstractSerialFormat(context), BinaryFormat {

   companion object {
      private val simpleModule = serializersModuleOf(mapOf(
         UUID::class to UUIDSerializer())
      )
      val default = Avro(simpleModule)
   }

   /**
    * Loads an instance of <T> from the given ByteArray, with the assumption that the record was stored
    * using [AvroFormat.DataFormat]. The schema used will be the embedded schema.
    */
   override fun <T> load(deserializer: DeserializationStrategy<T>, bytes: ByteArray): T =
      load(deserializer) {
         format = AvroFormat.DataFormat
      }.from(bytes).nextOrThrow()

   /**
    * Creates an [AvroInputStreamBuilder] that will read avro values.
    * Supply a function to this method to configure the builder, eg
    *
    * <pre>
    * val output = load<T>(serializer) {
    *    format = AvroFormat.DataFormat
    *    readerSchema = mySchema
    * }
    * </pre>
    *
    * When using formats [AvroFormat.JsonFormat] or [AvroFormat.BinaryFormat] then the
    * readerSchema must be set in the configuration function.
    */
   fun load(f: AvroInputStreamBuilder<Any>.() -> Unit): AvroInputStreamBuilder<Any> {
      val builder = AvroInputStreamBuilder { it }
      builder.f()
      return builder
   }

   /**
    * Creates an [AvroInputStreamBuilder] that will read instances of <T>.
    * Supply a function to this method to configure the builder, eg
    *
    * <pre>
    * val output = load<T>(serializer) {
    *    format = AvroFormat.DataFormat
    *    readerSchema = mySchema
    * }
    * </pre>
    *
    * When using formats [AvroFormat.JsonFormat] or [AvroFormat.BinaryFormat] then the
    * readerSchema must be set in the configuration function.
    */
   fun <T> load(serializer: DeserializationStrategy<T>,
                f: AvroInputStreamBuilder<T>.() -> Unit): AvroInputStreamBuilder<T> {
      val builder = AvroInputStreamBuilder { fromRecord(serializer, it as GenericRecord) }
      builder.f()
      return builder
   }

   /**
    * Writes an instance of <T> using a [Schema] derived from the type.
    * This method will use the [AvroFormat.DataFormat] format.
    * The written object will be returned as a [ByteArray].
    */
   override fun <T> dump(serializer: SerializationStrategy<T>, obj: T): ByteArray {
      val baos = ByteArrayOutputStream()
      val output = dump<T>(serializer) {
         format = AvroFormat.DataFormat
      }
      output.to(baos).write(obj).close()
      return baos.toByteArray()
   }

   /**
    * Creates an [AvroOutputStreamBuilder] that will write instances of <T>.
    * Supply a function to this method to configure the builder, eg
    *
    * <pre>
    * val output = dump<T>(serializer) {
    *    format = AvroFormat.DataFormat
    *    schema = mySchema
    * }
    * </pre>
    *
    */
   fun <T> dump(serializer: SerializationStrategy<T>,
                f: AvroOutputStreamBuilder<T>.() -> Unit): AvroOutputStreamBuilder<T> {
      val builder = AvroOutputStreamBuilder(serializer, this) { schema -> { toRecord(serializer, schema, it) } }
      builder.f()
      return builder
   }

   /**
    * Converts an instance of <T> to an Avro [Record] using a [Schema] derived from the type.
    */
   fun <T> toRecord(serializer: SerializationStrategy<T>,
                    obj: T): GenericRecord {
      return toRecord(serializer, schema(serializer), obj)
   }

   /**
    * Converts an instance of <T> to an Avro [Record] using the given [Schema].
    */
   fun <T> toRecord(serializer: SerializationStrategy<T>,
                    schema: Schema,
                    obj: T): GenericRecord {
      var record: Record? = null
      val encoder = RootRecordEncoder(schema, context) { record = it }
      encoder.encode(serializer, obj)
      return record!!
   }

   /**
    * Converts an Avro [GenericRecord] to an instance of <T> using the schema
    * present in the record.
    */
   fun <T> fromRecord(deserializer: DeserializationStrategy<T>,
                      record: GenericRecord): T {
      return RootRecordDecoder(record).decodeSerializableValue(deserializer)
   }

   fun <T> schema(serializer: SerializationStrategy<T>): Schema {
      return schemaFor(
         context,
         serializer.descriptor,
         serializer.descriptor.getEntityAnnotations(),
         DefaultNamingStrategy
      ).schema()
   }
}