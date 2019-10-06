package com.sksamuel.avro4k

import com.sksamuel.avro4k.serializer.BigDecimalSerializer
import com.sksamuel.avro4k.serializer.DateSerializer
import com.sksamuel.avro4k.serializer.InstantSerializer
import com.sksamuel.avro4k.serializer.LocalDateSerializer
import com.sksamuel.avro4k.serializer.LocalDateTimeSerializer
import com.sksamuel.avro4k.serializer.LocalTimeSerializer
import com.sksamuel.avro4k.serializer.TimestampSerializer
import com.sksamuel.avro4k.serializer.UUIDSerializer
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.StructureKind
import kotlinx.serialization.UnionKind
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.LogicalTypes
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

class ClassSchemaFor(private val context: SerialModule,
                     private val descriptor: SerialDescriptor) : SchemaFor {

   override fun schema(namingStrategy: NamingStrategy): Schema {

      val annos = AnnotationExtractor(descriptor.getEntityAnnotations())
      val naming = RecordNaming(descriptor)

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
      val naming = RecordNaming(descriptor, index)
      val schema = schemaFor(context, fieldDescriptor, descriptor.getElementAnnotations(index))
         .schema(DefaultNamingStrategy)

      // if we have annotated the field @AvroFixed then we override the type and change it to a Fixed schema
      // if someone puts @AvroFixed on a complex type, it makes no sense, but that's their cross to bear
      // in addition, someone could annotate the target type, so we need to check into that too
      val (size, name) = when (val a = annos.fixed()) {
         null -> {
            val fieldAnnos = AnnotationExtractor(fieldDescriptor.getEntityAnnotations())
            val fieldNaming = RecordNaming(fieldDescriptor)
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
      val naming = RecordNaming(descriptor)
      val symbols = (0 until descriptor.elementsCount).map { descriptor.getElementName(it) }
      return SchemaBuilder.enumeration(naming.name()).namespace(naming.namespace()).symbols(*symbols.toTypedArray())
   }
}

class PairSchemaFor(private val context: SerialModule,
                    private val descriptor: SerialDescriptor) : SchemaFor {
   override fun schema(namingStrategy: NamingStrategy): Schema {
      val a = schemaFor(context, descriptor.getElementDescriptor(0), descriptor.getElementAnnotations(0))
      val b = schemaFor(context, descriptor.getElementDescriptor(1), descriptor.getElementAnnotations(1))
      return SchemaBuilder.unionOf().type(a.schema(namingStrategy)).and().type(b.schema(namingStrategy)).endUnion()
   }
}

class ListSchemaFor(private val context: SerialModule,
                    private val descriptor: SerialDescriptor) : SchemaFor {
   override fun schema(namingStrategy: NamingStrategy): Schema {
      val elementType = descriptor.getElementDescriptor(0)
      return when (elementType.kind) {
         PrimitiveKind.BYTE -> SchemaBuilder.builder().bytesType()
         else -> {
            val elementSchema = schemaFor(context, elementType, descriptor.getElementAnnotations(0))
               .schema(namingStrategy)
            return Schema.createArray(elementSchema)
         }
      }
   }
}

class MapSchemaFor(private val context: SerialModule,
                   private val descriptor: SerialDescriptor) : SchemaFor {
   override fun schema(namingStrategy: NamingStrategy): Schema {
      val keyType = descriptor.getElementDescriptor(0)
      when (keyType.kind) {
         is PrimitiveKind.STRING -> {
            val valueType = descriptor.getElementDescriptor(1)
            val valueSchema = schemaFor(context, valueType, descriptor.getElementAnnotations(1)).schema(namingStrategy)
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

fun Schema.toSchemaFor() = SchemaFor.const(this)

fun schemaFor(context: SerialModule,
              descriptor: SerialDescriptor,
              annos: List<Annotation>): SchemaFor {

   val schemaFor: SchemaFor = when (descriptor.name) {
      UUIDSerializer.name -> LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().stringType()).toSchemaFor()
      DateSerializer.name -> LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()).toSchemaFor()
      TimestampSerializer.name -> LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType()).toSchemaFor()
      LocalDateTimeSerializer.name -> LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType()).toSchemaFor()
      LocalDateSerializer.name -> LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()).toSchemaFor()
      LocalTimeSerializer.name -> LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType()).toSchemaFor()
      InstantSerializer.name -> LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType()).toSchemaFor()
      BigDecimalSerializer.name -> {
         val (scale, precision) = AnnotationExtractor(annos).scalePrecision() ?: 2 to 8
         LogicalTypes.decimal(precision, scale).addToSchema(SchemaBuilder.builder().bytesType()).toSchemaFor()
      }
      else -> when (descriptor.kind) {
         PrimitiveKind.STRING -> SchemaFor.StringSchemaFor
         PrimitiveKind.LONG -> SchemaFor.LongSchemaFor
         PrimitiveKind.INT -> SchemaFor.IntSchemaFor
         PrimitiveKind.SHORT -> SchemaFor.ShortSchemaFor
         PrimitiveKind.BYTE -> SchemaFor.ByteSchemaFor
         PrimitiveKind.DOUBLE -> SchemaFor.DoubleSchemaFor
         PrimitiveKind.FLOAT -> SchemaFor.FloatSchemaFor
         PrimitiveKind.BOOLEAN -> SchemaFor.BooleanSchemaFor
         StructureKind.CLASS -> when (descriptor.name) {
            "kotlin.Pair" -> PairSchemaFor(context, descriptor)
            else -> ClassSchemaFor(context, descriptor)
         }
         UnionKind.ENUM_KIND -> EnumSchemaFor(descriptor)
         StructureKind.LIST -> ListSchemaFor(context, descriptor)
         StructureKind.MAP -> MapSchemaFor(context, descriptor)
         else -> throw SerializationException("Unsupported type ${descriptor.name} of ${descriptor.kind}")
      }
   }
   return if (descriptor.isNullable) NullableSchemaFor(schemaFor) else schemaFor
}

interface NamingStrategy {
   fun to(name: String): String = name
}

object DefaultNamingStrategy : NamingStrategy {
   override fun to(name: String): String = name
}