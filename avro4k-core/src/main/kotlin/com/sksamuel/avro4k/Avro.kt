package com.sksamuel.avro4k

import com.sksamuel.avro4k.decoder.RootRecordDecoder
import com.sksamuel.avro4k.encoder.RootRecordEncoder
import com.sksamuel.avro4k.io.AvroFormat
import com.sksamuel.avro4k.io.AvroInputStream
import com.sksamuel.avro4k.io.AvroOutputStream
import com.sksamuel.avro4k.schema.DefaultNamingStrategy
import com.sksamuel.avro4k.schema.schemaFor
import com.sksamuel.avro4k.serializer.UUIDSerializer
import kotlinx.serialization.AbstractSerialFormat
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.decode
import kotlinx.serialization.encode
import kotlinx.serialization.modules.EmptyModule
import kotlinx.serialization.modules.SerialModule
import kotlinx.serialization.modules.serializersModuleOf
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import java.io.ByteArrayOutputStream
import java.util.*

data class AvroConfiguration constructor(
   @JvmField internal val encodeDefaults: Boolean = true
)

class Avro(override val context: SerialModule = EmptyModule) : AbstractSerialFormat(context), BinaryFormat {

   companion object {
      private val simpleModule = serializersModuleOf(mapOf(
         UUID::class to UUIDSerializer())
      )
      val default = Avro(simpleModule)
   }

   /**
    * Loads an instance of <T> from the given ByteArray, with the assumption that the record was stored
    * using [AvroFormat.DataFormat]. If the byte array represents another format, create an instance
    * of the appropriate [AvroInputStream] using the functions on that companion object.
    */
   override fun <T> load(deserializer: DeserializationStrategy<T>, bytes: ByteArray): T {
      return AvroInputStream.data(deserializer).from(bytes).nextOrThrow()
   }

   /**
    * Writes values of <T> to a [ByteArray] using the given [Schema].
    */
   fun <T> dump(serializer: SerializationStrategy<T>, obj: T, schema: Schema): ByteArray {
      val baos = ByteArrayOutputStream()
      val output = AvroOutputStream.data(schema, serializer).to(baos)
      output.write(obj)
      output.close()
      return baos.toByteArray()
   }

   /**
    * Writes values of <T> using a [Schema] derived from the type.
    */
   override fun <T> dump(serializer: SerializationStrategy<T>, obj: T): ByteArray {
      return dump(serializer, obj, schema(serializer))
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
      return RootRecordDecoder(record).decode(deserializer)
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