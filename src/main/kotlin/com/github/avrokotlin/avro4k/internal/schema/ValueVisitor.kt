package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.SerializerLocatorMiddleware
import com.github.avrokotlin.avro4k.internal.jsonNode
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.serializer.AvroSchemaSupplier
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.nonNullOriginal
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.LogicalType
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

internal class ValueVisitor internal constructor(
    private val context: VisitorContext,
    private val onSchemaBuilt: (Schema) -> Unit,
) : SerialDescriptorValueVisitor {
    private var isNullable: Boolean = false

    override val serializersModule: SerializersModule
        get() = context.avro.serializersModule

    constructor(avro: Avro, onSchemaBuilt: (Schema) -> Unit) : this(
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

        val schema =
            SchemaBuilder.enumeration(descriptor.nonNullSerialName)
                .doc(annotations.doc?.value)
                .defaultSymbol(context.avro.enumResolver.getDefaultValueIndex(descriptor)?.let { descriptor.getElementName(it) })
                .symbols(*descriptor.elementNamesArray)

        annotations.aliases?.value?.forEach { schema.addAlias(it) }
        annotations.props.forEach { schema.addProp(it.key, it.jsonNode) }

        setSchema(schema)
    }

    private val SerialDescriptor.elementNamesArray: Array<String>
        get() = Array(elementsCount) { getElementName(it) }

    override fun visitObject(descriptor: SerialDescriptor) {
        // we consider objects as records without fields.
        visitClass(descriptor).endClassVisit(descriptor)
    }

    override fun visitClass(descriptor: SerialDescriptor) = ClassVisitor(descriptor, context.copy(inlinedElements = emptyList())) { setSchema(it) }

    override fun visitPolymorphic(
        descriptor: SerialDescriptor,
        kind: PolymorphicKind,
    ) = PolymorphicVisitor(context) { setSchema(it) }

    override fun visitList(descriptor: SerialDescriptor) = ListVisitor(context.copy(inlinedElements = emptyList())) { setSchema(it) }

    override fun visitMap(descriptor: SerialDescriptor) = MapVisitor(context.copy(inlinedElements = emptyList())) { setSchema(it) }

    override fun visitInlineClass(descriptor: SerialDescriptor) = InlineClassVisitor(context) { setSchema(it) }

    private fun setSchema(schema: Schema) {
        if (isNullable && !schema.isNullable) {
            onSchemaBuilt(schema.nullable)
        } else {
            onSchemaBuilt(schema)
        }
    }

    override fun visitValue(descriptor: SerialDescriptor) {
        val finalDescriptor = SerializerLocatorMiddleware.apply(unwrapNullable(descriptor))

        (finalDescriptor.nonNullOriginal as? AvroSchemaSupplier)
            ?.getSchema(context)
            ?.let {
                setSchema(it)
                return
            }
        super.visitValue(finalDescriptor)
    }

    private fun unwrapNullable(descriptor: SerialDescriptor): SerialDescriptor {
        if (descriptor.isNullable) {
            isNullable = true
            return descriptor.nonNullOriginal
        }
        return descriptor
    }
}

internal const val CHAR_LOGICAL_TYPE_NAME = "char"
private val CHAR_LOGICAL_TYPE = LogicalType(CHAR_LOGICAL_TYPE_NAME)

private fun PrimitiveKind.toSchema(): Schema =
    when (this) {
        PrimitiveKind.BOOLEAN -> Schema.create(Schema.Type.BOOLEAN)
        PrimitiveKind.CHAR -> Schema.create(Schema.Type.INT).also { CHAR_LOGICAL_TYPE.addToSchema(it) }
        PrimitiveKind.BYTE -> Schema.create(Schema.Type.INT)
        PrimitiveKind.SHORT -> Schema.create(Schema.Type.INT)
        PrimitiveKind.INT -> Schema.create(Schema.Type.INT)
        PrimitiveKind.LONG -> Schema.create(Schema.Type.LONG)
        PrimitiveKind.FLOAT -> Schema.create(Schema.Type.FLOAT)
        PrimitiveKind.DOUBLE -> Schema.create(Schema.Type.DOUBLE)
        PrimitiveKind.STRING -> Schema.create(Schema.Type.STRING)
    }