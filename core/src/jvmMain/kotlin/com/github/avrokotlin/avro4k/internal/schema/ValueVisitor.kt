package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroSchema
import com.github.avrokotlin.avro4k.AvroSchema.BooleanSchema
import com.github.avrokotlin.avro4k.AvroSchema.DoubleSchema
import com.github.avrokotlin.avro4k.AvroSchema.EnumSchema
import com.github.avrokotlin.avro4k.AvroSchema.FloatSchema
import com.github.avrokotlin.avro4k.AvroSchema.IntSchema
import com.github.avrokotlin.avro4k.AvroSchema.LongSchema
import com.github.avrokotlin.avro4k.AvroSchema.StringSchema
import com.github.avrokotlin.avro4k.Name
import com.github.avrokotlin.avro4k.fromApacheSchema
import com.github.avrokotlin.avro4k.fromJsonString
import com.github.avrokotlin.avro4k.internal.AvroGenerated
import com.github.avrokotlin.avro4k.internal.SerializerLocatorMiddleware
import com.github.avrokotlin.avro4k.internal.findAnnotation
import com.github.avrokotlin.avro4k.internal.jsonElement
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import com.github.avrokotlin.avro4k.isNullable
import com.github.avrokotlin.avro4k.nullable
import com.github.avrokotlin.avro4k.serializer.AvroSchemaSupplier
import com.github.avrokotlin.avro4k.serializer.stringable
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.nonNullOriginal
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.modules.SerializersModule

internal class ValueVisitor internal constructor(
    private val context: VisitorContext,
    private val onSchemaBuilt: (AvroSchema) -> Unit,
) : SerialDescriptorValueVisitor {
    private var isNullable: Boolean = false

    override val serializersModule: SerializersModule
        get() = context.avro.serializersModule

    constructor(avro: Avro, onSchemaBuilt: (AvroSchema) -> Unit) : this(
        VisitorContext(
            avro,
            mutableMapOf()
        ),
        onSchemaBuilt = onSchemaBuilt
    )

    override fun visitPrimitive(
        descriptor: SerialDescriptor,
        kind: PrimitiveKind,
    ) = setSchema(kind.toSchema())

    override fun visitEnum(descriptor: SerialDescriptor) {
        val annotations = TypeAnnotations(descriptor)

        val schema = EnumSchema(
            name = Name(descriptor.nonNullSerialName),
            symbols = descriptor.elementNamesArray.toList(),
            defaultSymbol = context.avro.enumResolver.getDefaultValueIndex(descriptor)?.let { descriptor.getElementName(it) },
            doc = annotations.doc?.value,
            aliases = annotations.aliases?.value?.map { Name(it) }?.toSet() ?: emptySet(),
            props = annotations.props.associate { it.key to it.jsonElement },
        )

        setSchema(schema)
    }

    private val SerialDescriptor.elementNamesArray: Array<String>
        get() = Array(elementsCount) { getElementName(it) }

    override fun visitObject(descriptor: SerialDescriptor) {
        // we consider objects as records without fields.
        visitClass(descriptor).endClassVisit(descriptor)
    }

    override fun visitClass(descriptor: SerialDescriptor) = ClassVisitor(descriptor, context.copy(inlinedElements = emptyList())) { setSchema(it) }

    @OptIn(ExperimentalSerializationApi::class)
    override fun visitPolymorphic(
        descriptor: SerialDescriptor,
        kind: PolymorphicKind,
    ) = PolymorphicVisitor(context) { setSchema(it) }

    override fun visitList(descriptor: SerialDescriptor) = ListVisitor(context.copy(inlinedElements = emptyList())) { setSchema(it) }

    override fun visitMap(descriptor: SerialDescriptor) = MapVisitor(context.copy(inlinedElements = emptyList())) { setSchema(it) }

    override fun visitInlineClass(descriptor: SerialDescriptor) = InlineClassVisitor(context) { setSchema(it) }

    private fun setSchema(schema: AvroSchema) {
        if (isNullable && !schema.isNullable) {
            onSchemaBuilt(schema.nullable)
        } else {
            onSchemaBuilt(schema)
        }
    }

    override fun visitValue(descriptor: SerialDescriptor) {
        val finalDescriptor = SerializerLocatorMiddleware.apply(unwrapNullable(descriptor))
        descriptor.findAnnotation<AvroGenerated>()?.let {
            // Ignore everything and use the provided schema
            setSchema(AvroSchema.fromJsonString(it.originalSchema))
            return
        }

        if (finalDescriptor is AvroSchemaSupplier) {
            setSchema(AvroSchema.fromApacheSchema(finalDescriptor.getSchema(context)))
            return
        }

        if (context.inlinedElements.any { it.stringable != null }) {
            setSchema(StringSchema())
            return
        }

        super.visitValue(finalDescriptor)
    }

    @OptIn(ExperimentalSerializationApi::class)
    private fun unwrapNullable(descriptor: SerialDescriptor): SerialDescriptor {
        if (descriptor.isNullable) {
            isNullable = true
            return descriptor.nonNullOriginal
        }
        return descriptor
    }
}

internal const val CHAR_LOGICAL_TYPE_NAME = "char"

private fun PrimitiveKind.toSchema(): AvroSchema =
    when (this) {
        PrimitiveKind.BOOLEAN -> BooleanSchema()
        PrimitiveKind.CHAR -> IntSchema(props = mapOf("logicalType" to JsonPrimitive(CHAR_LOGICAL_TYPE_NAME)))
        PrimitiveKind.BYTE -> IntSchema()
        PrimitiveKind.SHORT -> IntSchema()
        PrimitiveKind.INT -> IntSchema()
        PrimitiveKind.LONG -> LongSchema()
        PrimitiveKind.FLOAT -> FloatSchema()
        PrimitiveKind.DOUBLE -> DoubleSchema()
        PrimitiveKind.STRING -> StringSchema()
    }