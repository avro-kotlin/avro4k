package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.AvroStringable
import com.github.avrokotlin.avro4k.internal.findElementAnnotation
import com.github.avrokotlin.avro4k.internal.namespace
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema

/**
 * Base class for custom Avro serializers. It also provides a way to define custom Avro schema.
 *
 * Use it at your own risk, as it's directly bypassing the internal checks, so you can have runtime errors.
 *
 * Don't forget to implement [serializeGeneric] and [deserializeGeneric] if you want to use the serializer outside the Avro serialization, like with json format.
 */
public abstract class AvroSerializer<T>(
    descriptorName: String,
) : KSerializer<T>, AvroSchemaSupplier {
    @Suppress("LeakingThis")
    @OptIn(InternalSerializationApi::class)
    final override val descriptor: SerialDescriptor =
        SerialDescriptorWithAvroSchemaDelegate(buildSerialDescriptor(descriptorName, SerialKind.CONTEXTUAL), this)

    final override fun serialize(
        encoder: Encoder,
        value: T,
    ) {
        if (encoder is AvroEncoder) {
            serializeAvro(encoder, value)
            return
        }
        serializeGeneric(encoder, value)
    }

    final override fun deserialize(decoder: Decoder): T {
        if (decoder is AvroDecoder) {
            return deserializeAvro(decoder)
        }
        return deserializeGeneric(decoder)
    }

    /**
     * This method is called when the serializer is used outside Avro serialization.
     * By default, it throws an exception.
     *
     * Implement it to provide a generic serialization logic with the standard [Encoder].
     */
    public open fun serializeGeneric(
        encoder: Encoder,
        value: T,
    ) {
        throw UnsupportedOperationException("The serializer ${this::class.qualifiedName} is not usable outside of Avro serialization.")
    }

    /**
     * Serialize the value using an Avro encoder. It is highly recommended to use `encoder.encodeResolving` methods. See [AvroEncoder] for more details.
     */
    public abstract fun serializeAvro(
        encoder: AvroEncoder,
        value: T,
    )

    /**
     * This method is called when the serializer is used outside Avro serialization.
     * By default, it throws an exception.
     *
     * Implement it to provide a generic deserialization logic with the standard [Decoder].
     */
    public open fun deserializeGeneric(decoder: Decoder): T {
        throw UnsupportedOperationException("The serializer ${this::class.qualifiedName} is not usable outside of Avro serialization.")
    }

    /**
     * Deserialize the value from an Avro decoder. It is highly recommended to use `decoder.decodeResolvingXx` methods. See [AvroDecoder] for more details.
     */
    public abstract fun deserializeAvro(decoder: AvroDecoder): T
}

@ExperimentalSerializationApi
public interface SchemaSupplierContext {
    public val configuration: AvroConfiguration

    /**
     * Corresponds to the elements-tree, always starting from the data class property.
     *
     * The first element is the data class property, and the next elements are the inlined elements when the property type is a value class.
     */
    public val inlinedElements: List<ElementLocation>
}

/**
 * Search for the first annotation of type [T] in the given [ElementLocation].
 */
@ExperimentalSerializationApi
public inline fun <reified T : Annotation> ElementLocation.findAnnotation(): T? {
    return descriptor.findElementAnnotation(elementIndex)
}

/**
 * Shorthand for [findAnnotation] with [AvroDecimal] as it is a built-in annotation.
 */
@ExperimentalSerializationApi
public val ElementLocation.decimal: AvroDecimal?
    get() = findAnnotation()

/**
 * Shorthand for [findAnnotation] with [AvroStringable] as it is a built-in annotation.
 */
@ExperimentalSerializationApi
public val ElementLocation.stringable: AvroStringable?
    get() = findAnnotation()

/**
 * Creates a string schema from the [AvroStringable] annotation.
 */
@ExperimentalSerializationApi
public fun AvroStringable.createSchema(): Schema = Schema.create(Schema.Type.STRING)

/**
 * Shorthand for [findAnnotation] with [AvroFixed] as it is a built-in annotation.
 */
@ExperimentalSerializationApi
public val ElementLocation.fixed: AvroFixed?
    get() = findAnnotation()

/**
 * Creates a fixed schema from the [AvroFixed] annotation.
 */
@ExperimentalSerializationApi
public fun AvroFixed.createSchema(elementLocation: ElementLocation): Schema =
    Schema.createFixed(elementLocation.descriptor.getElementName(elementLocation.elementIndex), null, elementLocation.descriptor.namespace, size)

@ExperimentalSerializationApi
public data class ElementLocation
    @PublishedApi
    internal constructor(
        val descriptor: SerialDescriptor,
        val elementIndex: Int,
    )

internal fun interface AvroSchemaSupplier {
    fun getSchema(context: SchemaSupplierContext): Schema
}

internal class SerialDescriptorWithAvroSchemaDelegate(
    private val descriptor: SerialDescriptor,
    private val schemaSupplier: AvroSchemaSupplier,
) : SerialDescriptor by descriptor, AvroSchemaSupplier {
    override fun getSchema(context: SchemaSupplierContext): Schema {
        return schemaSupplier.getSchema(context)
    }

    override fun toString(): String {
        return "${descriptor.serialName}(<custom schema>)"
    }
}