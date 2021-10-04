

package com.github.avrokotlin.avro4k

import kotlinx.serialization.*
import kotlinx.serialization.modules.SerializersModule
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.starProjectedType

@OptIn(ExperimentalStdlibApi::class, ExperimentalSerializationApi::class)
fun <T : Any> SerializersModule.getPolymorphicInclSealedInterfaces(baseClass: KClass<in T>, value: T): SerializationStrategy<T> {
    //First try to get it from the module
    var result = this.getPolymorphic(baseClass, value)
    if(result == null && baseClass.isSealed && value::class.isSubclassOf(baseClass)) {
        result = this.serializer(value::class.starProjectedType)
    }
    return result ?: throwSubtypeNotRegistered(value::class, baseClass)
}

@OptIn(ExperimentalSerializationApi::class)
fun <T : Any> SerializersModule.getPolymorphicInclSealedInterfaces(baseClass: KClass<in T>, klassName: String?): DeserializationStrategy<out T>? {
    var deserializer = getPolymorphic(baseClass, klassName)
    if (deserializer == null && baseClass.isSealed) {
        deserializer = getSealedSubclassDeserializerOrNull(baseClass, klassName)
    }
    return deserializer
}

@Suppress("UNCHECKED_CAST")
fun <T : Any> SerializersModule.getSealedSubclassDeserializerOrNull(
    baseClass: KClass<in T>,
    klassName: String?
): DeserializationStrategy<out T>? {
    return baseClass.sealedSubclasses.asSequence()
        .flatMap { if (it.isSealed) it.sealedSubclasses.asSequence() else sequenceOf(it) }
        .filter { it.qualifiedName == klassName }.map { this.serializer(it.starProjectedType) }
        .firstOrNull() as? KSerializer<out T>
}


internal fun throwSubtypeNotRegistered(subClassName: String?, baseClass: KClass<*>): Nothing {
    val scope = "in the scope of '${baseClass.simpleName}'"
    throw SerializationException(
        if (subClassName == null)
            "Class discriminator was missing and no default polymorphic serializers were registered $scope"
        else
            "Class '$subClassName' is not registered for polymorphic serialization $scope.\n" +
                    "Mark the base class or implemented interface as 'sealed' or register the serializer explicitly."
    )
}


internal fun throwSubtypeNotRegistered(subClass: KClass<*>, baseClass: KClass<*>): Nothing =
    throwSubtypeNotRegistered(subClass.simpleName ?: "$subClass", baseClass)
