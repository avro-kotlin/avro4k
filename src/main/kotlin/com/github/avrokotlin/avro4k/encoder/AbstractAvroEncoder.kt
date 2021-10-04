package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.getPolymorphicInclSealedInterfaces
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.internal.AbstractPolymorphicSerializer
import kotlin.reflect.KClass

@Suppress("UNCHECKED_CAST")
@OptIn(ExperimentalSerializationApi::class)
abstract class AbstractAvroEncoder() : AbstractEncoder(){
    @OptIn(InternalSerializationApi::class, ExperimentalSerializationApi::class)
    override fun <T : Any?> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        if(serializer is AbstractPolymorphicSerializer<*> && value != null) {
            encodePolymorphicSerializableValue(serializer, value)
        } else{
            super.encodeSerializableValue(serializer, value)
        }
    }
    @OptIn(InternalSerializationApi::class)
    fun <S : Any> encodePolymorphicSerializableValue(serializer: AbstractPolymorphicSerializer<*>, value : S){
        //Special code to support sealed interfaces encoding as they are not natively supported in kotlinx.serialization
        val actualSerializer = serializersModule.getPolymorphicInclSealedInterfaces(serializer.baseClass as KClass<in S>, value)
        this.encodeStructure(serializer.descriptor) {
            encodeSerializableElement(serializer.descriptor, 1, actualSerializer, value)
        }
    }
}