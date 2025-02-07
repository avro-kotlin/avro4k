package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.serializer.AvroSchemaSupplier
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import com.github.avrokotlin.avro4k.serializer.SerialDescriptorWithAvroSchemaDelegate
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.descriptors.capturedKClass
import kotlinx.serialization.descriptors.elementDescriptors
import kotlinx.serialization.descriptors.getContextualDescriptor
import kotlinx.serialization.descriptors.getPolymorphicDescriptors
import kotlinx.serialization.descriptors.nonNullOriginal
import kotlinx.serialization.descriptors.nullable
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializerOrNull
import org.apache.avro.Schema

internal inline fun <reified T : Annotation> SerialDescriptor.findAnnotation() = annotations.firstNotNullOfOrNull { it as? T }

internal inline fun <reified T : Annotation> SerialDescriptor.findAnnotations() = annotations.filterIsInstance<T>()

@PublishedApi
internal inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotation(elementIndex: Int): T? = getElementAnnotations(elementIndex).firstNotNullOfOrNull { it as? T }

internal inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotations(elementIndex: Int) = getElementAnnotations(elementIndex).filterIsInstance<T>()

internal val SerialDescriptor.nonNullSerialName: String get() = nonNullOriginal.serialName
internal val SerialDescriptor.namespace: String? get() = serialName.substringBeforeLast('.', "").takeIf { it.isNotEmpty() }

internal val SerialDescriptor.aliases: Set<String>
    get() =
        findAnnotation<AvroAlias>()?.value?.toSet() ?: emptySet()

@ExperimentalSerializationApi
internal fun SerialDescriptor.possibleSerializationSubclasses(serializersModule: SerializersModule): Sequence<SerialDescriptor> {
    return when (this.kind) {
        PolymorphicKind.SEALED ->
            elementDescriptors.asSequence()
                .filter { it.kind == SerialKind.CONTEXTUAL }
                .flatMap { it.elementDescriptors }
                .flatMap { it.possibleSerializationSubclasses(serializersModule) }

        PolymorphicKind.OPEN ->
            serializersModule.getPolymorphicDescriptors(this@possibleSerializationSubclasses).asSequence()
                .flatMap { it.possibleSerializationSubclasses(serializersModule) }

        SerialKind.CONTEXTUAL -> sequenceOf(getNonNullContextualDescriptor(serializersModule))

        else -> sequenceOf(this)
    }
}

@OptIn(InternalSerializationApi::class)
internal fun SerialDescriptor.getNonNullContextualDescriptor(serializersModule: SerializersModule) =
    requireNotNull(serializersModule.getContextualDescriptor(this) ?: this.capturedKClass?.serializerOrNull()?.descriptor) {
        "No descriptor found in serialization context for $this"
    }

@OptIn(InternalSerializationApi::class)
internal fun Schema.descriptor(): SerialDescriptor {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (type) {
        Schema.Type.RECORD -> RecordSchemaSerialDescriptor(this)
        Schema.Type.ARRAY -> ArraySchemaSerialDescriptor(this)
        Schema.Type.MAP -> MapSchemaSerialDescriptor(this)
        Schema.Type.ENUM -> EnumSchemaSerialDescriptor(this)

        Schema.Type.UNION -> {
            if (types.isEmpty()) {
                throw SerializationException("Empty union schema is not supported")
            }
            var isNullable = false
            val nonNullableDescriptors =
                types.mapNotNull {
                    if (it.type == Schema.Type.NULL) {
                        isNullable = true
                        null
                    } else {
                        it.descriptor()
                    }
                }
            if (nonNullableDescriptors.isEmpty()) {
                throw SerializationException("Union schema with only a null type is not supported")
            } else if (nonNullableDescriptors.size == 1) {
                return nonNullableDescriptors.first()
            } else {
                buildSerialDescriptor("GenericUnion<${nonNullableDescriptors.joinToString { it.serialName }}>", PolymorphicKind.SEALED) {
                    element("type", String.serializer().descriptor)
                    element(
                        "value",
                        buildSerialDescriptor("union", SerialKind.CONTEXTUAL) {
                            nonNullableDescriptors.forEach {
                                element(it.serialName, it)
                            }
                        }
                    )
                }
            }.let { descriptor ->
                if (isNullable) {
                    descriptor.nullable
                } else {
                    descriptor
                }
            }
        }

        Schema.Type.FIXED -> TODO()
        Schema.Type.BYTES -> ByteArraySerializer().descriptor
        Schema.Type.STRING -> String.serializer().descriptor
        Schema.Type.INT -> Int.serializer().descriptor
        Schema.Type.LONG -> Long.serializer().descriptor
        Schema.Type.FLOAT -> Float.serializer().descriptor
        Schema.Type.DOUBLE -> Double.serializer().descriptor
        Schema.Type.BOOLEAN -> Boolean.serializer().descriptor
        Schema.Type.NULL -> nullDescriptor
    }.let { SerialDescriptorWithAvroSchemaDelegate(it) { this } }
}

private class RecordSchemaSerialDescriptor(private val schema: Schema) : SerialDescriptor, AvroSchemaSupplier {
    override val elementsCount: Int
        get() = schema.fields.size

    override val kind: SerialKind
        get() = StructureKind.CLASS

    override val serialName: String
        get() = schema.fullName

    override fun getElementAnnotations(index: Int): List<Annotation> = emptyList()

    override fun getElementDescriptor(index: Int): SerialDescriptor {
        return schema.fields[index].schema().descriptor()
    }

    override fun getElementIndex(name: String): Int {
        return schema.getField(name).pos()
    }

    override fun getElementName(index: Int): String {
        return schema.fields[index].name()
    }

    override fun isElementOptional(index: Int): Boolean {
        return !schema.fields[index].hasDefaultValue()
    }

    override fun getSchema(context: SchemaSupplierContext): Schema = schema
}

private class EnumSchemaSerialDescriptor(private val schema: Schema) : SerialDescriptor, AvroSchemaSupplier {
    override val elementsCount: Int
        get() = schema.enumSymbols.size

    override val kind: SerialKind
        get() = SerialKind.ENUM

    override val serialName: String
        get() = schema.fullName

    override fun getElementAnnotations(index: Int): List<Annotation> =
        if (schema.enumDefault == schema.enumSymbols[index]) {
            listOf(AvroEnumDefault())
        } else {
            emptyList()
        }

    @OptIn(InternalSerializationApi::class)
    override fun getElementDescriptor(index: Int): SerialDescriptor {
        return buildSerialDescriptor("${schema.fullName}.${schema.enumSymbols[index]}", StructureKind.OBJECT) {}
    }

    override fun getElementIndex(name: String): Int {
        return if (schema.hasEnumSymbol(name)) schema.getEnumOrdinal(name) else CompositeDecoder.UNKNOWN_NAME
    }

    override fun getElementName(index: Int): String {
        return schema.enumSymbols[index]
    }

    override fun isElementOptional(index: Int): Boolean {
        return schema.enumDefault == null
    }

    override fun getSchema(context: SchemaSupplierContext): Schema = schema
}

private class ArraySchemaSerialDescriptor(private val schema: Schema) : SerialDescriptor, AvroSchemaSupplier {
    private val elementDescriptor = schema.elementType.descriptor()

    override val elementsCount: Int
        get() = 1

    override val kind: SerialKind
        get() = StructureKind.LIST

    override val serialName: String
        get() = "GenericArray<${elementDescriptor.serialName}>"

    override fun getElementAnnotations(index: Int): List<Annotation> = emptyList()

    override fun getElementDescriptor(index: Int): SerialDescriptor {
        if (index == 0) {
            return elementDescriptor
        } else {
            throw IndexOutOfBoundsException("Array schema has only one element")
        }
    }

    override fun getElementIndex(name: String): Int {
        return if (name == "element") {
            0
        } else {
            CompositeDecoder.UNKNOWN_NAME
        }
    }

    override fun getElementName(index: Int): String {
        return if (index == 0) {
            "element"
        } else {
            throw IndexOutOfBoundsException("Array schema has only one element")
        }
    }

    override fun isElementOptional(index: Int): Boolean = false

    override fun getSchema(context: SchemaSupplierContext): Schema = schema
}

private class MapSchemaSerialDescriptor(private val schema: Schema) : SerialDescriptor, AvroSchemaSupplier {
    private val valueDescriptor = schema.valueType.descriptor()

    override val elementsCount: Int
        get() = 2

    override val kind: SerialKind
        get() = StructureKind.MAP

    override val serialName: String
        get() = "GenericMap<${valueDescriptor.serialName}>"

    override fun getElementAnnotations(index: Int): List<Annotation> = emptyList()

    override fun getElementDescriptor(index: Int): SerialDescriptor {
        return when (index) {
            0 -> String.serializer().descriptor
            1 -> valueDescriptor
            else -> throw IndexOutOfBoundsException("Map schema has only two elements")
        }
    }

    override fun getElementIndex(name: String): Int {
        return when (name) {
            "key" -> 0
            "value" -> 1
            else -> CompositeDecoder.UNKNOWN_NAME
        }
    }

    override fun getElementName(index: Int): String {
        return when (index) {
            0 -> "key"
            1 -> "value"
            else -> throw IndexOutOfBoundsException("Map schema has only two elements")
        }
    }

    override fun isElementOptional(index: Int): Boolean = false

    override fun getSchema(context: SchemaSupplierContext): Schema = schema
}

@OptIn(InternalSerializationApi::class)
private val nullDescriptor =
    SerialDescriptorWithAvroSchemaDelegate(buildSerialDescriptor("null", StructureKind.OBJECT) {}.nullable) {
        Schema.create(Schema.Type.NULL)
    }