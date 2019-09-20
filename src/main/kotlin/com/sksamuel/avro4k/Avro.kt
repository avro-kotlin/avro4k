package com.sksamuel.avro4k

import com.sksamuel.avro4k.serializers.BigDecimalSerializer
import kotlinx.serialization.AbstractSerialFormat
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.encode
import kotlinx.serialization.modules.EmptyModule
import kotlinx.serialization.modules.SerialModule
import kotlinx.serialization.modules.serializersModuleOf
import org.apache.avro.Schema
import java.io.ByteArrayOutputStream
import java.math.BigDecimal

data class AvroConfiguration constructor(
    @JvmField internal val encodeDefaults: Boolean = true
)

class Avro(override val context: SerialModule = EmptyModule) : AbstractSerialFormat(context), BinaryFormat {

  companion object {
    private val simpleModule = serializersModuleOf(BigDecimal::class, BigDecimalSerializer())
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

    val encoder = AvroEncoder(schema)
    encoder.encode(serializer, obj)

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
    val encoder = AvroEncoder(schema(serializer))
    encoder.encode(serializer, obj)
    return encoder.record()
  }

  fun <T> schema(serializer: SerializationStrategy<T>): Schema {
    return schemaFor(serializer.descriptor).schema(DefaultNamingStrategy)
  }
}