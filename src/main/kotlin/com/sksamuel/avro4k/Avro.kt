package com.sksamuel.avro4k

import kotlinx.serialization.AbstractSerialFormat
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ElementValueDecoder
import kotlinx.serialization.ElementValueEncoder
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.encode
import kotlinx.serialization.modules.EmptyModule
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema
import java.io.ByteArrayOutputStream

data class AvroConfiguration constructor(
    @JvmField internal val encodeDefaults: Boolean = true
)

class Avro(override val context: SerialModule = EmptyModule) : AbstractSerialFormat(context), BinaryFormat {

  companion object {
    val default = Avro()
  }

  override fun <T> load(deserializer: DeserializationStrategy<T>, bytes: ByteArray): T {
    TODO()
  }

  /**
   * Writes values of <T> using the supplied schema
   */
  fun <T> dump(serializer: SerializationStrategy<T>, obj: T, schema: Schema): ByteArray {

    val output = ByteArrayOutputStream()

    val encoder = AvroEncoder(output)
    encoder.encode(serializer, obj)

    return output.toByteArray()
  }

  /**
   * Writes values of <T> using a schema derived from the type.
   */
  override fun <T> dump(serializer: SerializationStrategy<T>, obj: T): ByteArray {
    return dump(serializer, obj, schema(serializer))
  }

  fun <T> schema(serializer: SerializationStrategy<T>): Schema {
    return schemaFor(serializer.descriptor).schema(DefaultNamingStrategy)
  }
}

class AvroEncoder(output: ByteArrayOutputStream) : ElementValueEncoder() {
}

class AvroDecoder() : ElementValueDecoder() {

}