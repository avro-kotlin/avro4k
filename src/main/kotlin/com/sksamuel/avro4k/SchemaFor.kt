package com.sksamuel.avro4k

import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerialKind
import kotlinx.serialization.StructureKind
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.nio.ByteBuffer

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

class ClassSchemaFor<T>(private val descriptor: SerialDescriptor) : SchemaFor<T> {
  override fun schema(namingStrategy: NamingStrategy): Schema {
    val builder = SchemaBuilder.record(descriptor.name)
        .doc(null)
        .namespace(descriptor.kind.javaClass.`package`.name)
        .fields()
    val builderWithProps = (0 until descriptor.elementsCount).fold(builder) { acc, index ->
      val schemaFor = schemaFor(descriptor.getElementDescriptor(index))
      acc.name(descriptor.getElementName(index)).type(schemaFor.schema(DefaultNamingStrategy)).noDefault()
    }
    return builderWithProps.endRecord()
  }
}

fun schemaFor(descriptor: SerialDescriptor) = when (descriptor.kind) {
  PrimitiveKind.STRING -> SchemaFor.StringSchemaFor
  PrimitiveKind.LONG -> SchemaFor.LongSchemaFor
  PrimitiveKind.INT -> SchemaFor.IntSchemaFor
  PrimitiveKind.SHORT -> SchemaFor.ShortSchemaFor
  PrimitiveKind.BYTE -> SchemaFor.ByteSchemaFor
  PrimitiveKind.DOUBLE -> SchemaFor.DoubleSchemaFor
  PrimitiveKind.FLOAT -> SchemaFor.FloatSchemaFor
  PrimitiveKind.BOOLEAN -> SchemaFor.BooleanSchemaFor
  StructureKind.CLASS -> ClassSchemaFor(descriptor)
  //-> SchemaFor.ByteArraySchemaFor
  /// ByteBuffer::class -> SchemaFor.ByteBufferSchemaFor
  else -> throw UnsupportedOperationException("Cannot find schemaFor for $descriptor")
}

interface NamingStrategy {
  fun to(name: String): String = name
}

object DefaultNamingStrategy : NamingStrategy {
  override fun to(name: String): String = name
}