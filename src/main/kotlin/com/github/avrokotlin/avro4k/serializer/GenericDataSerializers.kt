package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.UnionDecoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.builtins.ArraySerializer
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.descriptors.nullable
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.internal.AbstractCollectionSerializer
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.SerializersModuleBuilder
import kotlinx.serialization.modules.contextual
import kotlinx.serialization.serializerOrNull
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import kotlin.reflect.KClass

public val GenericDataSerializersModule: SerializersModule =
    SerializersModule {
        contextual(GenericDataSerializer)
        contextual(GenericRecordSerializer, GenericData.Record::class)
        contextual(GenericEnumSerializer, GenericData.EnumSymbol::class)
        contextual(GenericFixedSerializer, GenericData.Fixed::class)
        contextual(GenericArraySerializer, GenericData.Array::class)
    }

@Suppress("UNCHECKED_CAST")
private inline fun <reified T : Any, I : T> SerializersModuleBuilder.contextual(
    serializer: KSerializer<T>,
    implementationType: KClass<I>,
) {
    contextual(T::class, serializer)
    contextual(implementationType, serializer as KSerializer<I>)
}

@Suppress("UNCHECKED_CAST")
@OptIn(InternalSerializationApi::class)
public object GenericDataSerializer : AvroSerializer<Any>("GenericData") {
    private val mapSerializer: KSerializer<Any> = MapSerializer(GenericDataSerializer, GenericDataSerializer.nullable).wrapToCollectionSerializer()
    private val listSerializer: KSerializer<Any> = ListSerializer(GenericDataSerializer.nullable).wrapToCollectionSerializer()

    private fun KSerializer<*>.wrapToCollectionSerializer(): KSerializer<Any> {
        return AvroCollectionSerializer(this as AbstractCollectionSerializer<*, Any, *>)
    }

    // Only encoding, so we don't need AvroCollectionSerializer as it is only for decoding purposes
    private val arraySerializer = ArraySerializer(Any::class, GenericDataSerializer.nullable)

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: Any,
    ) {
        findSerializer(encoder, value).serialize(encoder, value)
    }

    private fun <T : Any> findSerializer(
        encoder: AvroEncoder,
        value: T,
    ): SerializationStrategy<Any> {
        return (
            when (value) {
                is Map<*, *> -> mapSerializer
                is Collection<*> -> listSerializer
                is Array<*> -> arraySerializer
                else -> encoder.serializersModule.serializerOrNull(value::class.java) ?: throw SerializationException("Could not find serializer for ${value::class}")
            } as SerializationStrategy<Any>
        ).let { encoder.avro.serializationMiddleware.apply(it) }
    }

    override fun deserializeAvro(decoder: AvroDecoder): Any {
        (decoder as UnionDecoder).decodeAndResolveUnion()

        decoder.currentWriterSchema.logicalType
            ?.let { decoder.avro.logicalTypeSerializers[it.name] }
            ?.let { return it.deserialize(decoder) }

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        return when (decoder.currentWriterSchema.type) {
            Schema.Type.DOUBLE -> decoder.decodeDouble()
            Schema.Type.BOOLEAN -> decoder.decodeBoolean()
            Schema.Type.STRING -> decoder.decodeString()
            Schema.Type.INT -> decoder.decodeInt()
            Schema.Type.LONG -> decoder.decodeLong()
            Schema.Type.FLOAT -> decoder.decodeFloat()
            Schema.Type.BYTES -> decoder.decodeBytes()
            Schema.Type.FIXED -> decoder.decodeFixed()
            Schema.Type.ENUM -> GenericEnumSerializer.deserializeAvro(decoder)
            Schema.Type.RECORD -> GenericRecordSerializer.deserializeAvro(decoder)
            Schema.Type.ARRAY -> listSerializer.deserialize(decoder)
            Schema.Type.MAP -> mapSerializer.deserialize(decoder)
            Schema.Type.UNION -> throw UnsupportedOperationException("union should be already resolved")
            Schema.Type.NULL -> throw UnsupportedOperationException("decode null")
        }
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException("Not possible to generate schema from generic data as it is only known at runtime")
    }
}

@ExperimentalSerializationApi
public object GenericRecordSerializer : AvroSerializer<GenericRecord>(GenericRecord::class.java.name) {
    override fun serializeAvro(
        encoder: AvroEncoder,
        value: GenericRecord,
    ) {
        val serialDescriptor = value.schema.descriptor()
        encoder.encodeStructure(serialDescriptor) {
            for (field in value.schema.fields) {
                encodeNullableSerializableElement(serialDescriptor, field.pos(), GenericDataSerializer, value[field.pos()])
            }
        }
    }

    override fun deserializeAvro(decoder: AvroDecoder): GenericRecord {
        val schema = decoder.currentWriterSchema
        val record = GenericData.Record(schema)
        val serialDescriptor = schema.descriptor()
        decoder.decodeStructure(serialDescriptor) {
            do {
                when (val index = decodeElementIndex(serialDescriptor)) {
                    CompositeDecoder.DECODE_DONE -> break
                    else -> record.put(index, decodeNullableSerializableElement(serialDescriptor, index, GenericDataSerializer))
                }
            } while (true)
        }
        return record
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException(
            "Not possible to generate schema from ${this::class.qualifiedName} as its schema is related to the serialized data itself. " +
                "Do not use ${Avro::class.qualifiedName}#${Avro::schema.name}() with this type."
        )
    }
}

@ExperimentalSerializationApi
public object GenericArraySerializer : AvroSerializer<GenericArray<*>>(GenericArray::class.java.name) {
    private val listSerializer = ListSerializer(GenericDataSerializer.nullable)

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: GenericArray<*>,
    ) {
        listSerializer.serialize(encoder, value)
    }

    override fun deserializeAvro(decoder: AvroDecoder): GenericArray<*> {
        return GenericData.Array(decoder.currentWriterSchema, listSerializer.deserialize(decoder))
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException(
            "Not possible to generate schema from ${this::class.qualifiedName} as its schema is related to the serialized data itself. " +
                "Do not use ${Avro::class.qualifiedName}#${Avro::schema.name}() with this type."
        )
    }
}

@ExperimentalSerializationApi
public object GenericEnumSerializer : AvroSerializer<GenericEnumSymbol<*>>(GenericEnumSymbol::class.java.name) {
    override fun serializeAvro(
        encoder: AvroEncoder,
        value: GenericEnumSymbol<*>,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeAvro(decoder: AvroDecoder): GenericEnumSymbol<*> {
        return GenericData.EnumSymbol(decoder.currentWriterSchema, decoder.decodeString())
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException(
            "Not possible to generate schema from ${this::class.qualifiedName} as its schema is related to the serialized data itself. " +
                "Do not use ${Avro::class.qualifiedName}#${Avro::schema.name}() with this type."
        )
    }
}

@ExperimentalSerializationApi
public object GenericFixedSerializer : AvroSerializer<GenericFixed>(GenericFixed::class.java.name) {
    override fun serializeAvro(
        encoder: AvroEncoder,
        value: GenericFixed,
    ) {
        encoder.encodeFixed(value)
    }

    override fun deserializeAvro(decoder: AvroDecoder): GenericFixed {
        return decoder.decodeFixed()
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException(
            "Not possible to generate schema from ${GenericFixed::class.qualifiedName} as its schema is related to the serialized data itself. " +
                "Do not use ${Avro::class.qualifiedName}#${Avro::schema.name}() with this type."
        )
    }
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

        Schema.Type.FIXED -> GenericFixedSerializer.descriptor
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