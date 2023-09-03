package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AnnotationExtractor
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroConfiguration
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
import kotlinx.serialization.modules.SerializersModule
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
   private val descriptor: SerialDescriptor
) : SchemaFor {
   override fun schema(): Schema {
      val naming = RecordNaming(descriptor, DefaultNamingStrategy)
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
                    private val configuration: AvroConfiguration,
                    private val serializersModule: SerializersModule,
                    private val resolvedSchemas: MutableMap<RecordNaming, Schema>
) : SchemaFor {

   override fun schema(): Schema {
      val a = schemaFor(
         serializersModule,
         descriptor.getElementDescriptor(0),
         descriptor.getElementAnnotations(0),
         configuration,
         resolvedSchemas
      )
      val b = schemaFor(
         serializersModule,
         descriptor.getElementDescriptor(1),
         descriptor.getElementAnnotations(1),
         configuration,
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
                    private val serializersModule: SerializersModule,
                    private val configuration: AvroConfiguration,
                    private val resolvedSchemas: MutableMap<RecordNaming, Schema>
) : SchemaFor {

   override fun schema(): Schema {

      val elementType = descriptor.getElementDescriptor(0) // don't use unwrapValueClass to prevent losing serial annotations
      return when (descriptor.unwrapValueClass.getElementDescriptor(0).kind) {
         PrimitiveKind.BYTE -> SchemaBuilder.builder().bytesType()
         else -> {
            val elementSchema = schemaFor(serializersModule,
               elementType,
               descriptor.getElementAnnotations(0),
               configuration,
               resolvedSchemas
            ).schema()
            return Schema.createArray(elementSchema)
         }
      }
   }
}

@ExperimentalSerializationApi
class MapSchemaFor(private val descriptor: SerialDescriptor,
                   private val serializersModule: SerializersModule,
                   private val configuration: AvroConfiguration,
                   private val resolvedSchemas: MutableMap<RecordNaming, Schema>
) : SchemaFor {

   override fun schema(): Schema {
      val keyType = descriptor.getElementDescriptor(0).unwrapValueClass
      when (keyType.kind) {
         is PrimitiveKind.STRING -> {
            val valueType = descriptor.getElementDescriptor(1)
            val valueSchema = schemaFor(
               serializersModule,
               valueType,
               descriptor.getElementAnnotations(1),
               configuration,
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
fun schemaFor(serializersModule: SerializersModule,
              descriptor: SerialDescriptor,
              annos: List<Annotation>,
              configuration: AvroConfiguration,
              resolvedSchemas: MutableMap<RecordNaming, Schema>
): SchemaFor {

   val underlying = if (descriptor.javaClass.simpleName == "SerialDescriptorForNullable") {
      val field = descriptor.javaClass.getDeclaredField("original")
      field.isAccessible = true
      field.get(descriptor) as SerialDescriptor
   } else descriptor

   val schemaFor: SchemaFor = when (underlying) {
      is AvroDescriptor -> SchemaFor.const(underlying.schema(annos, serializersModule, configuration.namingStrategy))
      else -> when (descriptor.unwrapValueClass.kind) {
         PrimitiveKind.STRING -> SchemaFor.StringSchemaFor
         PrimitiveKind.LONG -> SchemaFor.LongSchemaFor
         PrimitiveKind.INT -> SchemaFor.IntSchemaFor
         PrimitiveKind.SHORT -> SchemaFor.ShortSchemaFor
         PrimitiveKind.BYTE -> SchemaFor.ByteSchemaFor
         PrimitiveKind.DOUBLE -> SchemaFor.DoubleSchemaFor
         PrimitiveKind.FLOAT -> SchemaFor.FloatSchemaFor
         PrimitiveKind.BOOLEAN -> SchemaFor.BooleanSchemaFor
         SerialKind.ENUM -> EnumSchemaFor(descriptor)
         SerialKind.CONTEXTUAL -> schemaFor(
            serializersModule,
            requireNotNull(
               serializersModule.getContextualDescriptor(descriptor.unwrapValueClass)
                  ?: descriptor.capturedKClass?.serializerOrNull()?.descriptor
            ) {
               "Contextual or default serializer not found for $descriptor "
            },
            annos,
            configuration,
            resolvedSchemas
         )

         StructureKind.CLASS, StructureKind.OBJECT -> when (descriptor.serialName) {
            "kotlin.Pair" -> PairSchemaFor(descriptor, configuration, serializersModule, resolvedSchemas)
            else -> ClassSchemaFor(descriptor, configuration, serializersModule, resolvedSchemas)
         }

         StructureKind.LIST -> ListSchemaFor(descriptor, serializersModule, configuration, resolvedSchemas)
         StructureKind.MAP -> MapSchemaFor(descriptor, serializersModule, configuration, resolvedSchemas)
         is PolymorphicKind -> UnionSchemaFor(descriptor, configuration, serializersModule, resolvedSchemas)
         else -> throw SerializationException("Unsupported type ${descriptor.serialName} of ${descriptor.kind}")
      }
   }

   return if (descriptor.isNullable) NullableSchemaFor(schemaFor, annos) else schemaFor
}

// copy-paste from kotlinx serialization because it internal
@ExperimentalSerializationApi
internal val SerialDescriptor.unwrapValueClass: SerialDescriptor
   get() = if (isInline) getElementDescriptor(0) else this