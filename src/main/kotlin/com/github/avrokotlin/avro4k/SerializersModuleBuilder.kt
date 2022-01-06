@file:OptIn(InternalSerializationApi::class)
@file:Suppress("UNCHECKED_CAST")
package com.github.avrokotlin.avro4k
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.modules.SerializersModuleBuilder
import kotlinx.serialization.serializer
import kotlin.reflect.KClass

/**
 * Registers all direct subclasses of a sealed type as polymorphic types.
 *
 * Warning: This method will be removed with no further warning as soon as kotlinx.serialization natively supports
 * sealed interfaces.
 */
fun <T : Any> SerializersModuleBuilder.polymorphicForSealed(sealedType: KClass<T>) {
    if (!sealedType.isSealed) throw IllegalArgumentException(
        "Type $sealedType is not sealed."
    )
    sealedType.sealedSubclasses.forEach {
        polymorphic(sealedType, it as KClass<T>, it.serializer())
    }
}


/**
 * Registers all subclasses of a sealed type tree as polymorphic types. In contrast to polymorphicForSealed this method
 * also adds subclasses of sealed subclasses as polymorphic types.
 *
 * Warning: This method will be removed with no further warning as soon as kotlinx.serialization natively supports
 * sealed interfaces.
 */
fun <T : Any> SerializersModuleBuilder.polymorphicTreeForSealed(sealedType: KClass<T>) {
    if (!sealedType.isSealed) throw IllegalArgumentException(
        "Type $sealedType is not a sealed type."
    )
    sealedType.sealedSubclasses.forEach {
        polymorphic(sealedType, it as KClass<T>, it.serializer())
        if(it.isSealed) {
            polymorphicTreeForSealed(it)
        }
    }
}

