package com.sksamuel.avro4k

import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.StructureKind
import kotlinx.serialization.UnionKind
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

    val fields = (0 until descriptor.elementsCount).map { index ->

      // the field can override the containingNamespace if the Namespace annotation is present on the field
      // we may have annotated our field with @AvroNamespace so this containingNamespace should be applied
      // to any schemas we have generated for this field
      // val schemaWithResolvedNamespace = extractor.namespace
      //   .map(SchemaHelper.overrideNamespace(schemaWithOrderedUnion, _))
      // .getOrElse(schemaWithOrderedUnion)

      val schema = schemaFor(descriptor.getElementDescriptor(index)).schema(DefaultNamingStrategy)

      val field = Schema.Field(descriptor.getElementName(index), schema, null, null)

      val props = descriptor.getElementAnnotations(index).filterIsInstance<AvroProp>()
      props.forEach { field.addProp(it.key, it.value) }
      field
    }

    val naming = NameExtractor(descriptor)
    val record = Schema.createRecord(naming.name(), null, naming.namespace(), false)
    record.fields = fields

    // aliases.foreach(record.addAlias)

    val props = descriptor.getEntityAnnotations().filterIsInstance<AvroProp>()
    props.forEach { record.addProp(it.key, it.value) }

    return record
  }
}

class EnumSchemaFor<T>(private val descriptor: SerialDescriptor) : SchemaFor<T> {
  override fun schema(namingStrategy: NamingStrategy): Schema {
    val naming = NameExtractor(descriptor)
    val symbols = (0 until descriptor.elementsCount).map { descriptor.getElementName(it) }
    return SchemaBuilder.enumeration(naming.name()).namespace(naming.namespace()).symbols(*symbols.toTypedArray())
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
  UnionKind.ENUM_KIND -> EnumSchemaFor(descriptor)
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