package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.SerializersModuleCollector
import kotlin.reflect.KClass


/**
 * Collects all serializers that support logical types. If there are multiple serializers for the same logical type,
 * only the last one will be kept.
 */
internal fun SerializersModule.collectLogicalTypesMapping(): Map<String, KSerializer<out Any>> {
    val result = mutableMapOf<String, KSerializer<out Any>>()
    dumpTo(object : SerializersModuleCollector {
        override fun <T : Any> contextual(kClass: KClass<T>, serializer: KSerializer<T>) {
            if (serializer is AvroSerializer<*>) {
                serializer.supportedLogicalTypes.forEach { supportedLogicalTypeName ->
                    result[supportedLogicalTypeName] = serializer
                }
            }
        }

        override fun <T : Any> contextual(kClass: KClass<T>, provider: (typeArgumentsSerializers: List<KSerializer<*>>) -> KSerializer<*>) {
            // Logical types are only supported with scalar non-generic serializers
        }

        override fun <Base : Any> polymorphicDefaultDeserializer(baseClass: KClass<Base>, defaultDeserializerProvider: (className: String?) -> DeserializationStrategy<Base>?) {
            // Logical types are only supported with scalar non-generic serializers
        }

        override fun <Base : Any, Sub : Base> polymorphic(baseClass: KClass<Base>, actualClass: KClass<Sub>, actualSerializer: KSerializer<Sub>) {
            // Logical types are only supported with scalar non-generic serializers
        }

        override fun <Base : Any> polymorphicDefaultSerializer(baseClass: KClass<Base>, defaultSerializerProvider: (value: Base) -> SerializationStrategy<Base>?) {
            // Logical types are only supported with scalar non-generic serializers
        }
    })
    return result
}
