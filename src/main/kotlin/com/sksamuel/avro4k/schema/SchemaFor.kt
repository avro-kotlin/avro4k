package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.RecordNaming
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.StructureKind
import kotlinx.serialization.UnionKind
import kotlinx.serialization.modules.SerialModule
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
      val a = schemaFor(context,
         descriptor.getElementDescriptor(0),
         descriptor.getElementAnnotations(0))
      val b = schemaFor(context,
         descriptor.getElementDescriptor(1),
         descriptor.getElementAnnotations(1))
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
            val elementSchema = schemaFor(context,
               elementType,
               descriptor.getElementAnnotations(0))
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
            val valueSchema = schemaFor(context,
               valueType,
               descriptor.getElementAnnotations(1)).schema(namingStrategy)
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

   val schemaFor: SchemaFor = when (descriptor) {
      is AvroDescriptor -> SchemaFor.const(descriptor.schema(annos))
      else -> when (descriptor.kind) {
         PrimitiveKind.STRING -> SchemaFor.StringSchemaFor
         PrimitiveKind.LONG -> SchemaFor.LongSchemaFor
         PrimitiveKind.INT -> SchemaFor.IntSchemaFor
         PrimitiveKind.SHORT -> SchemaFor.ShortSchemaFor
         PrimitiveKind.BYTE -> SchemaFor.ByteSchemaFor
         PrimitiveKind.DOUBLE -> SchemaFor.DoubleSchemaFor
         PrimitiveKind.FLOAT -> SchemaFor.FloatSchemaFor
         PrimitiveKind.BOOLEAN -> SchemaFor.BooleanSchemaFor
         UnionKind.ENUM_KIND -> EnumSchemaFor(descriptor)
         UnionKind.SEALED -> SealedClassSchemaFor(descriptor)
         StructureKind.CLASS -> when (descriptor.name) {
            "kotlin.Pair" -> PairSchemaFor(context, descriptor)
            else -> ClassSchemaFor(context, descriptor)
         }
         StructureKind.LIST -> ListSchemaFor(context, descriptor)
         StructureKind.MAP -> MapSchemaFor(context, descriptor)
         else -> throw SerializationException("Unsupported type ${descriptor.name} of ${descriptor.kind}")
      }
   }

   return if (descriptor.isNullable) NullableSchemaFor(schemaFor) else schemaFor
}