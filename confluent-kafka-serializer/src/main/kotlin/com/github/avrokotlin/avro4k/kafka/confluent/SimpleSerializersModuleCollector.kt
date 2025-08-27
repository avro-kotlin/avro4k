package com.github.avrokotlin.avro4k.kafka.confluent

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.modules.SerializersModuleCollector
import kotlin.reflect.KClass

@OptIn(ExperimentalSerializationApi::class)
internal interface SimpleSerializersModuleCollector : SerializersModuleCollector {
    override fun <T : Any> contextual(kClass: KClass<T>, provider: (typeArgumentsSerializers: List<KSerializer<*>>) -> KSerializer<*>) {
    }

    override fun <Base : Any, Sub : Base> polymorphic(baseClass: KClass<Base>, actualClass: KClass<Sub>, actualSerializer: KSerializer<Sub>) {
    }

    override fun <Base : Any> polymorphicDefaultDeserializer(baseClass: KClass<Base>, defaultDeserializerProvider: (className: String?) -> DeserializationStrategy<Base>?) {
    }

    override fun <Base : Any> polymorphicDefaultSerializer(baseClass: KClass<Base>, defaultSerializerProvider: (value: Base) -> SerializationStrategy<Base>?) {
    }
}