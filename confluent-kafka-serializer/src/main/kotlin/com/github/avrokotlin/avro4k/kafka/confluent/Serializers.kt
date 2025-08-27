package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.MissingFieldsEncodingException
import com.github.avrokotlin.avro4k.internal.aliases
import com.github.avrokotlin.avro4k.internal.isNamedSchema
import com.github.avrokotlin.avro4k.namedSchemaNotFoundInUnionError
import com.github.avrokotlin.avro4k.serializer.AnySerializer
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import com.github.avrokotlin.avro4k.trySelectNamedSchema
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.nonNullOriginal
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializerOrNull
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.SpecificData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer
import java.util.WeakHashMap
import kotlin.reflect.KClass


internal fun Avro.withAnyKSerializer(kSerializer: AnySerializer): Avro =
    Avro(from = this) {
        serializersModule = SerializersModule {
            contextual(Any::class, kSerializer)
        }
    }

internal class ReflectKSerializer : AnySerializer() {
    override fun SerializersModule.inferSerializationStrategyFromNonSerializableType(type: Class<out Any>) = inferSerializationStrategy(type)

    override fun SerializersModule.preResolveDeserializationStrategy(writerSchema: Schema) =
        writerSchema.getProp(SpecificData.CLASS_PROP)
            ?.let { tryGetJavaClass(it) }
            ?.let { serializerOrNull(it) }

    override fun SerializersModule.resolveEnumDeserializationStrategy(writerSchema: Schema) =
        resolveNamedSchema(writerSchema) ?: GenericEnumKSerializer

    override fun SerializersModule.resolveRecordDeserializationStrategy(writerSchema: Schema) =
        resolveNamedSchema(writerSchema) ?: GenericRecordKSerializer(this@ReflectKSerializer)

    override fun SerializersModule.resolveFixedDeserializationStrategy(writerSchema: Schema) =
        resolveNamedSchema(writerSchema) ?: GenericFixedKSerializer

    private fun SerializersModule.resolveNamedSchema(writerSchema: Schema): DeserializationStrategy<Any>? =
        findRegisteredDeserializerFromModule(writerSchema) ?: findDeserializerFromJavaClassPath(writerSchema)

    private fun SerializersModule.findDeserializerFromJavaClassPath(writerSchema: Schema): DeserializationStrategy<Any>? = iterator {
        yield(writerSchema.fullName)
        yieldAll(writerSchema.aliases)
    }.asSequence()
        .mapNotNull { tryGetJavaClass(it) }
        .firstNotNullOfOrNull { serializerOrNull(it) }

    private fun SerializersModule.findRegisteredDeserializerFromModule(schema: Schema): DeserializationStrategy<Any>? {
        if (!schema.isNamedSchema()) return null

        val names = mutableMapOf<String, DeserializationStrategy<*>>()
        @OptIn(ExperimentalSerializationApi::class)
        dumpTo(object : SimpleSerializersModuleCollector {
            private fun dumpNames(deserializationStrategy: DeserializationStrategy<*>) {
                names[deserializationStrategy.descriptor.nonNullOriginal.serialName] = deserializationStrategy
                deserializationStrategy.descriptor.aliases.forEach {
                    names[it] = deserializationStrategy
                }
            }

            override fun <T : Any> contextual(kClass: KClass<T>, serializer: KSerializer<T>) {
                dumpNames(serializer)
            }

            override fun <T : Any> contextual(
                kClass: KClass<T>,
                provider: (typeArgumentsSerializers: List<KSerializer<*>>) -> KSerializer<*>
            ) {
                dumpNames(provider(kClass.typeParameters.map { this@ReflectKSerializer }))
            }

            override fun <Base : Any, Sub : Base> polymorphic(
                baseClass: KClass<Base>,
                actualClass: KClass<Sub>,
                actualSerializer: KSerializer<Sub>
            ) {
                dumpNames(actualSerializer)
            }

            override fun <Base : Any> polymorphicDefaultDeserializer(
                baseClass: KClass<Base>,
                defaultDeserializerProvider: (className: String?) -> DeserializationStrategy<Base>?
            ) {
                defaultDeserializerProvider(null)?.let { dumpNames(it) }
            }
        })
        @Suppress("UNCHECKED_CAST")
        return (names[schema.fullName] ?: schema.aliases.asSequence().mapNotNull { names[it] }.firstOrNull()) as DeserializationStrategy<Any>?
    }

    private fun tryGetJavaClass(name: String): Class<*>? = runCatching { Class.forName(name) }.getOrNull()
}

internal class GenericKSerializer : AnySerializer() {
    override fun SerializersModule.inferSerializationStrategyFromNonSerializableType(type: Class<out Any>) = inferSerializationStrategy(type)
    override fun SerializersModule.resolveEnumDeserializationStrategy(writerSchema: Schema) = GenericEnumKSerializer
    override fun SerializersModule.resolveFixedDeserializationStrategy(writerSchema: Schema) = GenericFixedKSerializer
    override fun SerializersModule.resolveRecordDeserializationStrategy(writerSchema: Schema) = GenericRecordKSerializer(this@GenericKSerializer)
}

private fun AnySerializer.inferSerializationStrategy(type: Class<out Any>): SerializationStrategy<*>? = when {
    GenericFixed::class.java.isAssignableFrom(type) -> GenericFixedKSerializer
    GenericEnumSymbol::class.java.isAssignableFrom(type) -> GenericEnumKSerializer
    IndexedRecord::class.java.isAssignableFrom(type) -> GenericRecordKSerializer(this)
    ByteBuffer::class.java.isAssignableFrom(type) -> ByteBufferKSerializer
    Utf8::class.java.isAssignableFrom(type) -> Utf8KSerializer
    else -> null
}

internal object GenericEnumKSerializer : AvroSerializer<GenericEnumSymbol<*>>(GenericEnumSymbol::class.qualifiedName!!) {
    override fun deserializeAvro(decoder: AvroDecoder): GenericEnumSymbol<*> {
        return GenericData.EnumSymbol(decoder.currentWriterSchema, decoder.decodeString())
    }

    override fun serializeAvro(encoder: AvroEncoder, value: GenericEnumSymbol<*>) {
        if (encoder.currentWriterSchema.isUnion) {
            encoder.trySelectNamedSchema(value.schema.fullName, value.schema::getAliases)
            // When unable to determine the type from the enum schema, delegate it to the native encodeString resolver instead of raising an error here
        }
        encoder.encodeString(value.toString())
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw schemaGenerationUnsupportedError()
    }
}

internal object GenericFixedKSerializer : AvroSerializer<GenericFixed>(GenericFixed::class.qualifiedName!!) {
    override fun deserializeAvro(decoder: AvroDecoder): GenericFixed {
        return decoder.decodeFixed()
    }

    override fun serializeAvro(encoder: AvroEncoder, value: GenericFixed) {
        with(encoder) {
            if (currentWriterSchema.isUnion) {
                trySelectNamedSchema(value.schema.fullName, value.schema::getAliases)
                // When unable to determine the type from the fixed schema, delegate it to the native encodeFixed resolver instead of raising an error here
            }
            encodeFixed(value.bytes())
        }
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw schemaGenerationUnsupportedError()
    }
}

internal class GenericRecordKSerializer(
    private val anySerializer: AnySerializer,
) : AvroSerializer<IndexedRecord>(IndexedRecord::class.qualifiedName!!) {
    private val cache = WeakHashMap<Schema, SerialDescriptor>()

    override fun serializeAvro(encoder: AvroEncoder, value: IndexedRecord) {
        with(encoder) {
            if (currentWriterSchema.isUnion) {
                trySelectNamedSchema(value.schema.fullName, value.schema::getAliases) ||
                        throw namedSchemaNotFoundInUnionError(value.schema.fullName, value.schema.aliases)
            }
            val descriptor = value.schema.toGenericDescriptor()
            encodeStructure(descriptor) {
                (0 until descriptor.elementsCount).forEach { index ->
                    val fieldValue = value.get(index)
                    if (fieldValue == null && !descriptor.getElementDescriptor(index).isNullable) {
                        // In IndexedRecord, the implementation can miss a field, which in that case is represented by null,
                        // and .get() does not fail when the field is null even if not authorized.
                        // so we need to fail here, or we will get a "cannot write null to a non-nullable field" error later.
                        throw MissingFieldsEncodingException(listOf(value.schema.fields[index]), value.schema)
                    }
                    @OptIn(ExperimentalSerializationApi::class)
                    encodeNullableSerializableElement(descriptor, index, anySerializer.nullable, fieldValue)
                }
            }
        }
    }

    override fun deserializeAvro(decoder: AvroDecoder): IndexedRecord {
        with(decoder) {
            val descriptor = currentWriterSchema.toGenericDescriptor()
            return decodeStructure(descriptor) {
                val result = GenericData.Record(currentWriterSchema)
                var index: Int
                do {
                    index = decodeElementIndex(descriptor)
                    if (index == CompositeDecoder.DECODE_DONE) break
                    @OptIn(ExperimentalSerializationApi::class)
                    result.put(index, decodeNullableSerializableElement(descriptor, index, anySerializer))
                } while (true)
                result
            }
        }
    }

    private fun Schema.toGenericDescriptor(): SerialDescriptor =
        cache.computeIfAbsent(this) {
            buildClassSerialDescriptor(fullName) {
                fields.forEach { field ->
                    // Let kotlinx serialization know about the field's nullability.
                    val fieldDescriptor =
                        if (field.schema().isNullable) anySerializer.nullable.descriptor
                        else anySerializer.descriptor
                    element(
                        field.name(),
                        fieldDescriptor,
                        annotations = buildList {
                            field.aliases().takeIf { it.isNotEmpty() }?.let {
                                add(AvroAlias(*it.toTypedArray()))
                            }
                        }
                    )
                }
                @OptIn(ExperimentalSerializationApi::class)
                annotations = buildList {
                    aliases.takeIf { it.isNotEmpty() }?.let {
                        add(AvroAlias(*it.toTypedArray()))
                    }
                }
            }
        }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw schemaGenerationUnsupportedError()
    }
}

internal object Utf8KSerializer : SerializationStrategy<Utf8> {
    override val descriptor: SerialDescriptor
        get() = throw UnsupportedOperationException()

    override fun serialize(encoder: Encoder, value: Utf8) {
        encoder as AvroEncoder
        encoder.encodeString(value.toString())
    }
}

internal object ByteBufferKSerializer : SerializationStrategy<ByteBuffer> {
    override val descriptor: SerialDescriptor
        get() = throw UnsupportedOperationException()

    override fun serialize(encoder: Encoder, value: ByteBuffer) {
        encoder as AvroEncoder
        encoder.encodeBytes(value.readRemainingBytes())
    }

    private fun ByteBuffer.readRemainingBytes(): ByteArray {
        if (hasArray() && position() == 0) {
            val array = array()
            if (array.size == remaining()) {
                // If the ByteBuffer is backed by an array and the position is at the start,
                // we can return the entire array directly.
                return array
            }
        }
        val bytes = ByteArray(remaining())
        val pos = position()
        get(bytes)
        position(pos)
        return bytes
    }
}

private fun KSerializer<*>.schemaGenerationUnsupportedError(): UnsupportedOperationException =
    UnsupportedOperationException("${this::class} cannot be used to get a schema")
