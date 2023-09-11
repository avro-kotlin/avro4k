package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AnnotationExtractor
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.RecordNaming
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.capturedKClass
import kotlinx.serialization.descriptors.elementNames
import kotlinx.serialization.descriptors.getContextualDescriptor
import kotlinx.serialization.serializerOrNull
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

interface SchemaFor {

   fun schema(): Schema

   companion object {

      /**
       * Creates a [SchemaFor] that always returns the given constant schema.
       */
      fun const(schema: Schema) = object : SchemaFor {
         override fun schema() = schema
      }

      val StringSchemaFor: SchemaFor = const(SchemaBuilder.builder().stringType())
      val LongSchemaFor: SchemaFor = const(SchemaBuilder.builder().longType())
      val IntSchemaFor: SchemaFor = const(SchemaBuilder.builder().intType())
      val ShortSchemaFor: SchemaFor = const(SchemaBuilder.builder().intType())
      val ByteSchemaFor: SchemaFor = const(SchemaBuilder.builder().intType())
      val DoubleSchemaFor: SchemaFor = const(SchemaBuilder.builder().doubleType())
      val FloatSchemaFor: SchemaFor = const(SchemaBuilder.builder().floatType())
      val BooleanSchemaFor: SchemaFor = const(SchemaBuilder.builder().booleanType())
   }
}

@ExperimentalSerializationApi
class EnumSchemaFor(
   private val descriptor: SerialDescriptor,
   private val avro: Avro,
) : SchemaFor {
   override fun schema(): Schema {
      val naming = avro.nameResolver.resolveTypeName(descriptor)
      val entityAnnotations = AnnotationExtractor(descriptor.annotations)
      val symbols = (0 until descriptor.elementsCount).map { descriptor.getElementName(it) }

      val defaultSymbol = entityAnnotations.enumDefault()?.let { enumDefault ->
         descriptor.elementNames.firstOrNull { it == enumDefault } ?: error(
            "Could not use: $enumDefault to resolve the enum class ${descriptor.serialName}"
         )
      }

      val enumSchema = SchemaBuilder.enumeration(naming.name).doc(entityAnnotations.doc())
         .namespace(naming.namespace)
         .defaultSymbol(defaultSymbol)
         .symbols(*symbols.toTypedArray())

      entityAnnotations.aliases().forEach { enumSchema.addAlias(it) }

      return enumSchema
   }
}

@ExperimentalSerializationApi
class PairSchemaFor(private val descriptor: SerialDescriptor,
                    private val avro: Avro,
                    private val resolvedSchemas: MutableMap<RecordNaming, Schema>
) : SchemaFor {

   override fun schema(): Schema {
      val a = schemaFor(
          avro,
         descriptor.getElementDescriptor(0),
         descriptor.getElementAnnotations(0),
         resolvedSchemas
      )
      val b = schemaFor(
         avro,
         descriptor.getElementDescriptor(1),
         descriptor.getElementAnnotations(1),
         resolvedSchemas
      )
      return SchemaBuilder.unionOf()
         .type(a.schema())
         .and()
         .type(b.schema())
         .endUnion()
   }
}

@ExperimentalSerializationApi
class ListSchemaFor(private val descriptor: SerialDescriptor,
                    private val avro: Avro,
                    private val resolvedSchemas: MutableMap<RecordNaming, Schema>
) : SchemaFor {

   override fun schema(): Schema {

      val elementType = descriptor.getElementDescriptor(0) // don't use unwrapValueClass to prevent losing serial annotations
      return when (descriptor.unwrapValueClass.getElementDescriptor(0).kind) {
         PrimitiveKind.BYTE -> SchemaBuilder.builder().bytesType()
         else -> {
            val elementSchema = schemaFor(avro,
               elementType,
               descriptor.getElementAnnotations(0),
               resolvedSchemas
            ).schema()
            return Schema.createArray(elementSchema)
         }
      }
   }
}

@ExperimentalSerializationApi
class MapSchemaFor(private val descriptor: SerialDescriptor,
                   private val avro: Avro,
                   private val resolvedSchemas: MutableMap<RecordNaming, Schema>
) : SchemaFor {

   override fun schema(): Schema {
      val keyType = descriptor.getElementDescriptor(0).unwrapValueClass
      when (keyType.kind) {
         is PrimitiveKind.STRING -> {
            val valueType = descriptor.getElementDescriptor(1)
            val valueSchema = schemaFor(
               avro,
               valueType,
               descriptor.getElementAnnotations(1),
               resolvedSchemas
            ).schema()
            return Schema.createMap(valueSchema)
         }

         else -> throw RuntimeException("Avro only supports STRING as the key type in a MAP")
      }
   }
}

@ExperimentalSerializationApi
class NullableSchemaFor(
   private val schemaFor: SchemaFor,
   private val annotations: List<Annotation>,
) : SchemaFor {

   private val nullFirst by lazy {
      //The default value can only be of the first type in the union definition.
      //Therefore we have to check the default value in order to decide the order of types within the union.
      //If no default is set, or if the default value is of type "null", nulls will be first.
      val default = AnnotationExtractor(annotations).default()
      default == null || default == Avro.NULL
   }

   override fun schema(): Schema {
      val elementSchema = schemaFor.schema()
      val nullSchema = SchemaBuilder.builder().nullType()
      return createSafeUnion(nullFirst, elementSchema, nullSchema)
   }
}

@OptIn(InternalSerializationApi::class)
@ExperimentalSerializationApi
fun schemaFor(avro: Avro,
              descriptor: SerialDescriptor,
              annos: List<Annotation>,
              resolvedSchemas: MutableMap<RecordNaming, Schema>
): SchemaFor {

   val underlying = if (descriptor.javaClass.simpleName == "SerialDescriptorForNullable") {
      val field = descriptor.javaClass.getDeclaredField("original")
      field.isAccessible = true
      field.get(descriptor) as SerialDescriptor
   } else descriptor

   val schemaFor: SchemaFor = when (underlying) {
      is AvroDescriptor -> SchemaFor.const(underlying.schema(annos, avro.serializersModule, avro.configuration.namingStrategy))
      else -> when (descriptor.unwrapValueClass.kind) {
         PrimitiveKind.STRING -> SchemaFor.StringSchemaFor
         PrimitiveKind.LONG -> SchemaFor.LongSchemaFor
         PrimitiveKind.INT -> SchemaFor.IntSchemaFor
         PrimitiveKind.SHORT -> SchemaFor.ShortSchemaFor
         PrimitiveKind.BYTE -> SchemaFor.ByteSchemaFor
         PrimitiveKind.DOUBLE -> SchemaFor.DoubleSchemaFor
         PrimitiveKind.FLOAT -> SchemaFor.FloatSchemaFor
         PrimitiveKind.BOOLEAN -> SchemaFor.BooleanSchemaFor
         SerialKind.ENUM -> EnumSchemaFor(descriptor, avro)
         SerialKind.CONTEXTUAL -> schemaFor(
            avro,
            requireNotNull(
               avro.serializersModule.getContextualDescriptor(descriptor.unwrapValueClass)
                  ?: descriptor.capturedKClass?.serializerOrNull()?.descriptor
            ) {
               "Contextual or default serializer not found for $descriptor "
            },
            annos,
            resolvedSchemas
         )

         StructureKind.CLASS, StructureKind.OBJECT -> when (descriptor.serialName) {
            "kotlin.Pair" -> PairSchemaFor(descriptor, avro, resolvedSchemas)
            else -> ClassSchemaFor(descriptor, avro, resolvedSchemas)
         }

         StructureKind.LIST -> ListSchemaFor(descriptor, avro, resolvedSchemas)
         StructureKind.MAP -> MapSchemaFor(descriptor, avro, resolvedSchemas)
         is PolymorphicKind -> UnionSchemaFor(descriptor, avro, resolvedSchemas)
         else -> throw SerializationException("Unsupported type ${descriptor.serialName} of ${descriptor.kind}")
      }
   }

   return if (descriptor.isNullable) NullableSchemaFor(schemaFor, annos) else schemaFor
}

// copy-paste from kotlinx serialization because it internal
@ExperimentalSerializationApi
internal val SerialDescriptor.unwrapValueClass: SerialDescriptor
   get() = if (isInline) getElementDescriptor(0) else this