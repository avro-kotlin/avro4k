package com.sksamuel.avro4k

import com.sksamuel.avro4k.serializers.UUIDSerializer
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

   override fun <T> load(deserializer: DeserializationStrategy<T>, bytes: ByteArray): T {
      TODO()
   }

   /**
    * Writes values of <T> to a [ByteArray] using the given [Schema].
    */
   fun <T> dump(serializer: SerializationStrategy<T>, obj: T, schema: Schema): ByteArray {

      val output = ByteArrayOutputStream()

  //    val encoder = RecordEncoder(schema)
  //    encoder.encode(serializer, obj)

      return output.toByteArray()
   }

   /**
    * Writes values of <T> using a [Schema] derived from the type.
    */
   override fun <T> dump(serializer: SerializationStrategy<T>, obj: T): ByteArray {
      return dump(serializer, obj, schema(serializer))
   }

   /**
    * Convert instance of <T> to an Avro [Record] using a [Schema] derived from the type.
    */
   fun <T> toRecord(serializer: SerializationStrategy<T>,
                    obj: T,
                    namingStrategy: NamingStrategy = DefaultNamingStrategy): Record {
      return toRecord(serializer, schema(serializer), obj, namingStrategy)
   }

   /**
    * Convert instance of <T> to an Avro [Record] using the given [Schema].
    */
   fun <T> toRecord(serializer: SerializationStrategy<T>,
                    schema: Schema,
                    obj: T,
                    namingStrategy: NamingStrategy = DefaultNamingStrategy): Record {
      var record: Record? = null
      val encoder = RecordEncoder(schema, context) { record = it }
      encoder.encode(serializer, obj)
      return record!!
   }

   fun <T> schema(serializer: SerializationStrategy<T>): Schema {
      return schemaFor(context, serializer.descriptor, serializer.descriptor.getEntityAnnotations())
         .schema(DefaultNamingStrategy)
   }

   fun <T> fromRecord(deserializer: DeserializationStrategy<T>,
                      record: GenericRecord): T {
      return RecordDecoder(record).decode(deserializer)
   }

   fun <T> fromRecord(deserializer: DeserializationStrategy<T>,
                      schema: Schema,
                      record: GenericRecord): T {
      return RecordDecoder(record).decode(deserializer)
   }
}