package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.internal.AvroLogicalTypeSupplier
import com.github.avrokotlin.avro4k.internal.AvroSchemaGenerationException
import com.github.avrokotlin.avro4k.internal.jsonNode
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
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
    private var logicalType: LogicalType? = null

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
        // we consider objects as records without fields since the beginning. Is it really a good idea ?
        visitClass(descriptor).endClassVisit(descriptor)
    }

    override fun visitClass(descriptor: SerialDescriptor) = ClassVisitor(descriptor, context.resetNesting()) { setSchema(it) }

    override fun visitPolymorphic(
        descriptor: SerialDescriptor,
        kind: PolymorphicKind,
    ) = PolymorphicVisitor(context) { setSchema(it) }

    override fun visitList(descriptor: SerialDescriptor) = ListVisitor(context.copy(inlinedAnnotations = null)) { setSchema(it) }

    override fun visitMap(descriptor: SerialDescriptor) = MapVisitor(context.copy(inlinedAnnotations = null)) { setSchema(it) }

    override fun visitInlineClass(descriptor: SerialDescriptor) = InlineClassVisitor(context) { setSchema(it) }

    private fun setSchema(schema: Schema) {
        val finalSchema = logicalType?.addToSchema(schema) ?: schema
        if (isNullable && !finalSchema.isNullable) {
            onSchemaBuilt(finalSchema.toNullableSchema())
        } else {
            onSchemaBuilt(finalSchema)
        }
    }

    private fun visitByteArray() {
        setSchema(Schema.create(Schema.Type.BYTES))
    }

    private fun visitFixed(fixed: AnnotatedElementOrType<AvroFixed>) {
        val parentFieldName =
            fixed.elementIndex?.let { fixed.descriptor.getElementName(it) }
                ?: throw AvroSchemaGenerationException("@AvroFixed must be used on a field")
        val parentNamespace = fixed.descriptor.serialName.substringBeforeLast('.', "").takeIf { it.isNotEmpty() }

        setSchema(
            SchemaBuilder.fixed(parentFieldName)
                .namespace(parentNamespace)
                .size(fixed.annotation.size)
        )
    }

    override fun visitValue(descriptor: SerialDescriptor) {
        if (descriptor.isNullable) {
            isNullable = true
        }
        if (descriptor.kind == SerialKind.CONTEXTUAL) {
            super.visitValue(descriptor)
            return
        }
        val annotations = context.inlinedAnnotations.appendAnnotations(ValueAnnotations(descriptor))

        (descriptor.nonNullOriginal as? AvroLogicalTypeSupplier)?.let {
            logicalType = it.getLogicalType(annotations.stack)
        }
        when {
            annotations.fixed != null -> visitFixed(annotations.fixed)
            descriptor.isByteArray() -> visitByteArray()
            else -> super.visitValue(descriptor)
        }
    }
}

private fun Schema.toNullableSchema(): Schema {
    return if (this.type == Schema.Type.UNION) {
        Schema.createUnion(listOf(Schema.create(Schema.Type.NULL)) + this.types)
    } else {
        Schema.createUnion(Schema.create(Schema.Type.NULL), this)
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

private fun SerialDescriptor.isByteArray(): Boolean = kind == StructureKind.LIST && getElementDescriptor(0).let { !it.isNullable && it.kind == PrimitiveKind.BYTE }