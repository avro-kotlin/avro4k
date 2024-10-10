package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.serializer.AvroSchemaSupplier
import com.github.avrokotlin.avro4k.serializer.ElementLocation
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.modules.SerializersModuleCollector
import kotlin.reflect.KClass

internal class LogicalTypesSerializersCollector(
    private val configuration: AvroConfiguration,
) : SerializersModuleCollector {
    internal var serializers: MutableMap<String, KSerializer<Any>> = mutableMapOf()

    override fun <T : Any> contextual(
        kClass: KClass<T>,
        serializer: KSerializer<T>,
    ) {
        if (serializer !is AvroSchemaSupplier || serializer.descriptor.isNullable) {
            return
        }
        val schema = runCatching { serializer.getSchema(SimpleContext(configuration)) }.getOrNull()
        if (schema?.logicalType != null) {
            @Suppress("UNCHECKED_CAST")
            serializers[schema.logicalType.name] = serializer as KSerializer<Any>
        }
    }

    override fun <T : Any> contextual(
        kClass: KClass<T>,
        provider: (typeArgumentsSerializers: List<KSerializer<*>>) -> KSerializer<*>,
    ) {
    }

    override fun <Base : Any, Sub : Base> polymorphic(
        baseClass: KClass<Base>,
        actualClass: KClass<Sub>,
        actualSerializer: KSerializer<Sub>,
    ) {
    }

    override fun <Base : Any> polymorphicDefaultDeserializer(
        baseClass: KClass<Base>,
        defaultDeserializerProvider: (className: String?) -> DeserializationStrategy<Base>?,
    ) {
    }

    override fun <Base : Any> polymorphicDefaultSerializer(
        baseClass: KClass<Base>,
        defaultSerializerProvider: (value: Base) -> SerializationStrategy<Base>?,
    ) {
    }
}

private class SimpleContext(
    override val configuration: AvroConfiguration,
    override val inlinedElements: List<ElementLocation> = emptyList(),
) : SchemaSupplierContext