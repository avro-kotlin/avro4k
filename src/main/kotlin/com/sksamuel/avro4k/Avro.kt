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
import org.apache.avro.SchemaBuilder
import java.io.ByteArrayOutputStream
import java.lang.UnsupportedOperationException
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties

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
    return when (obj) {
      is Any -> dump(serializer, obj, schema((obj as Any)::class))
      else -> throw UnsupportedOperationException()
    }
  }

  inline fun <reified T : Any> schema() = schema(T::class)

  fun <T : Any> schema(klass: KClass<T>): Schema {
    require(klass.isData)
    val builder = SchemaBuilder.record(klass.simpleName)
        .doc(null)
        .namespace(klass.java.`package`.name)
        .fields()
    val builderWithProps = klass.memberProperties.fold(builder) { acc, prop ->
      val schemaFor = schemaFor(prop.returnType)
      acc.name(prop.name).type(schemaFor.schema(DefaultNamingStrategy)).noDefault()
    }
    return builderWithProps.endRecord()
  }
}

class AvroEncoder(output: ByteArrayOutputStream) : ElementValueEncoder() {
}

class AvroDecoder() : ElementValueDecoder() {

}