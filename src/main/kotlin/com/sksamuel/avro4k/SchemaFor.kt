package com.sksamuel.avro4k

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.nio.ByteBuffer
import kotlin.reflect.KType

interface SchemaFor<T> {
  fun schema(namingStrategy: NamingStrategy): Schema

  companion object {

    /**
     * Creates a [SchemaFor] that always returns the given constant schema.
     */
    fun <T> const(schema: Schema) = object : SchemaFor<T> {
      override fun schema(namingStrategy: NamingStrategy) = schema
    }

    val StringSchemaFor: SchemaFor<String> = const(SchemaBuilder.builder().stringType())
    val LongSchemaFor: SchemaFor<Long> = const(SchemaBuilder.builder().longType())
    val IntSchemaFor: SchemaFor<Int> = const(SchemaBuilder.builder().intType())
    val ShortSchemaFor: SchemaFor<Short> = const(SchemaBuilder.builder().intType())
    val ByteSchemaFor: SchemaFor<Byte> = const(SchemaBuilder.builder().intType())
    val DoubleSchemaFor: SchemaFor<Double> = const(SchemaBuilder.builder().doubleType())
    val FloatSchemaFor: SchemaFor<Float> = const(SchemaBuilder.builder().floatType())
    val BooleanSchemaFor: SchemaFor<Boolean> = const(SchemaBuilder.builder().booleanType())
    val ByteArraySchemaFor: SchemaFor<Array<Byte>> = const(SchemaBuilder.builder().bytesType())
    val ByteBufferSchemaFor: SchemaFor<ByteBuffer> = const(SchemaBuilder.builder().bytesType())
  }
}

fun schemaFor(ktype: KType) = when (ktype.classifier) {
  String::class -> SchemaFor.StringSchemaFor
  Long::class -> SchemaFor.LongSchemaFor
  Int::class -> SchemaFor.IntSchemaFor
  Short::class -> SchemaFor.ShortSchemaFor
  Byte::class -> SchemaFor.ByteSchemaFor
  Double::class -> SchemaFor.DoubleSchemaFor
  Float::class -> SchemaFor.FloatSchemaFor
  Boolean::class -> SchemaFor.BooleanSchemaFor
  Array<Byte>::class -> SchemaFor.ByteArraySchemaFor
  ByteBuffer::class -> SchemaFor.ByteBufferSchemaFor
  else -> throw UnsupportedOperationException()
}

interface NamingStrategy {
  fun to(name: String): String = name
}

object DefaultNamingStrategy : NamingStrategy {
  override fun to(name: String): String = name
}