package com.sksamuel.avro4k

import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.StructureKind
import kotlinx.serialization.UnionKind
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

interface SchemaFor {
   fun schema(namingStrategy: NamingStrategy): Schema

   companion object {

      /**
       * Creates a [SchemaFor] that always returns the given constant schema.
       */
      fun const(schema: Schema) = object : SchemaFor {
         override fun schema(namingStrategy: NamingStrategy) = schema
      }

      val StringSchemaFor: SchemaFor = const(SchemaBuilder.builder().stringType())
      val LongSchemaFor: SchemaFor = const(SchemaBuilder.builder().longType())
      val IntSchemaFor: SchemaFor = const(SchemaBuilder.builder().intType())
      val ShortSchemaFor: SchemaFor = const(SchemaBuilder.builder().intType())
      val ByteSchemaFor: SchemaFor = const(SchemaBuilder.builder().intType())
      val DoubleSchemaFor: SchemaFor = const(SchemaBuilder.builder().doubleType())
      val FloatSchemaFor: SchemaFor = const(SchemaBuilder.builder().floatType())
      val BooleanSchemaFor: SchemaFor = const(SchemaBuilder.builder().booleanType())
      val ByteArraySchemaFor: SchemaFor = const(SchemaBuilder.builder().bytesType())
      val ByteBufferSchemaFor: SchemaFor = const(SchemaBuilder.builder().bytesType())
   }
}

class ClassSchemaFor(private val descriptor: SerialDescriptor) : SchemaFor {

   override fun schema(namingStrategy: NamingStrategy): Schema {

      val annos = AnnotationExtractor(descriptor.getEntityAnnotations())
      val naming = NameExtractor(descriptor)

      val fields = (0 until descriptor.elementsCount)
         .filterNot { descriptor.isElementOptional(it) }
         .map { index -> buildField(index, naming.namespace()) }

      val record = Schema.createRecord(naming.name(), annos.doc(), naming.namespace(), false)
      record.fields = fields
      annos.aliases().forEach { record.addAlias(it) }
      annos.props().forEach { (k, v) -> record.addProp(k, v) }
      return record
   }

   fun buildField(index: Int, containingNamespace: String): Schema.Field {

      val fieldDescriptor = descriptor.getElementDescriptor(index)
      val annos = AnnotationExtractor(descriptor.getElementAnnotations(index))
      val naming = NameExtractor(descriptor, index)
      val schema = schemaFor(fieldDescriptor).schema(DefaultNamingStrategy)

      // if we have annotated the field @AvroFixed then we override the type and change it to a Fixed schema
      // if someone puts @AvroFixed on a complex type, it makes no sense, but that's their cross to bear
      // in addition, someone could annotate the target type, so we need to check into that too
      val (size, name) = when (val a = annos.fixed()) {
         null -> {
            val fieldAnnos = AnnotationExtractor(fieldDescriptor.getEntityAnnotations())
            val fieldNaming = NameExtractor(fieldDescriptor)
            when (val b = fieldAnnos.fixed()) {
               null -> 0 to naming.name()
               else -> b to fieldNaming.name()
            }
         }
         else -> a to naming.name()
      }
      val schemaOrFixed = when (size) {
         0 -> schema
         else ->
            SchemaBuilder
               .fixed(name)
               .doc(annos.doc())
               .namespace(annos.namespace() ?: containingNamespace)
               .size(size)
      }

      // the field can override the containingNamespace if the Namespace annotation is present on the field
      // we may have annotated our field with @AvroNamespace so this containingNamespace should be applied
      // to any schemas we have generated for this field
      val schemaWithResolvedNamespace = when (val ns = annos.namespace()) {
         null -> schemaOrFixed
         else -> schemaOrFixed.overrideNamespace(ns)
      }

      val field = Schema.Field(naming.name(), schemaWithResolvedNamespace, annos.doc(), null)
      val props = this.descriptor.getElementAnnotations(index).filterIsInstance<AvroProp>()
      props.forEach { field.addProp(it.key, it.value) }
      annos.aliases().forEach { field.addAlias(it) }

      return field
   }
}

class EnumSchemaFor(private val descriptor: SerialDescriptor) : SchemaFor {
   override fun schema(namingStrategy: NamingStrategy): Schema {
      val naming = NameExtractor(descriptor)
      val symbols = (0 until descriptor.elementsCount).map { descriptor.getElementName(it) }
      return SchemaBuilder.enumeration(naming.name()).namespace(naming.namespace()).symbols(*symbols.toTypedArray())
   }
}

class PairSchemaFor(private val descriptor: SerialDescriptor) : SchemaFor {
   override fun schema(namingStrategy: NamingStrategy): Schema {
      val a = schemaFor(descriptor.getElementDescriptor(0)).schema(namingStrategy)
      val b = schemaFor(descriptor.getElementDescriptor(1)).schema(namingStrategy)
      return SchemaBuilder.unionOf().type(a).and().type(b).endUnion()
   }
}

class ListSchemaFor(private val descriptor: SerialDescriptor) : SchemaFor {
   override fun schema(namingStrategy: NamingStrategy): Schema {
      val elementType = descriptor.getElementDescriptor(0)
      return when (elementType.kind) {
         PrimitiveKind.BYTE -> SchemaBuilder.builder().bytesType()
         else -> {
            val elementSchema = schemaFor(elementType).schema(namingStrategy)
            return Schema.createArray(elementSchema)
         }
      }
   }
}

class MapSchemaFor(private val descriptor: SerialDescriptor) : SchemaFor {
   override fun schema(namingStrategy: NamingStrategy): Schema {
      val keyType = descriptor.getElementDescriptor(0)
      when (keyType.kind) {
         is PrimitiveKind.STRING -> {
            val valueType = descriptor.getElementDescriptor(1)
            val valueSchema = schemaFor(valueType).schema(namingStrategy)
            return Schema.createMap(valueSchema)
         }
         else -> throw RuntimeException("Avro only supports STRING as the key type in a MAP")
      }
   }
}

class NullableSchemaFor(private val schemaFor: SchemaFor) : SchemaFor {
   override fun schema(namingStrategy: NamingStrategy): Schema {
      val elementSchema = schemaFor.schema(namingStrategy)
      val nullSchema = SchemaBuilder.builder().nullType()
      return SchemaHelper.createSafeUnion(elementSchema, nullSchema)
   }
}

fun schemaFor(descriptor: SerialDescriptor): SchemaFor {
   val schemaFor = when (descriptor.kind) {
      PrimitiveKind.STRING -> SchemaFor.StringSchemaFor
      PrimitiveKind.LONG -> SchemaFor.LongSchemaFor
      PrimitiveKind.INT -> SchemaFor.IntSchemaFor
      PrimitiveKind.SHORT -> SchemaFor.ShortSchemaFor
      PrimitiveKind.BYTE -> SchemaFor.ByteSchemaFor
      PrimitiveKind.DOUBLE -> SchemaFor.DoubleSchemaFor
      PrimitiveKind.FLOAT -> SchemaFor.FloatSchemaFor
      PrimitiveKind.BOOLEAN -> SchemaFor.BooleanSchemaFor
      StructureKind.CLASS -> when (descriptor.name) {
         "kotlin.Pair" -> PairSchemaFor(descriptor)
         else -> ClassSchemaFor(descriptor)
      }
      UnionKind.ENUM_KIND -> EnumSchemaFor(descriptor)
      StructureKind.LIST -> ListSchemaFor(descriptor)
      StructureKind.MAP -> MapSchemaFor(descriptor)
      else -> throw UnsupportedOperationException("Cannot find schemaFor for ${descriptor.kind}")
   }
   return if (descriptor.isNullable) NullableSchemaFor(schemaFor) else schemaFor
}

interface NamingStrategy {
   fun to(name: String): String = name
}

object DefaultNamingStrategy : NamingStrategy {
   override fun to(name: String): String = name
}