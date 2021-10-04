package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.findPolymorphicSerializerInclSealedInterfaceOrNull
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.PolymorphicSerializer
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.internal.AbstractPolymorphicSerializer

@OptIn(ExperimentalSerializationApi::class)
abstract class AbstractAvroDecoder(): AbstractDecoder() {


    @OptIn(InternalSerializationApi::class)
    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return if(deserializer is PolymorphicSerializer<*>) {
            decodePolymorphicSerializableValue(deserializer)
        } else{
            super.decodeSerializableValue(deserializer)
        }
    }
    @Suppress("UNCHECKED_CAST")
    @OptIn(InternalSerializationApi::class)
    fun <S : Any> decodePolymorphicSerializableValue(serializer: AbstractPolymorphicSerializer<*>) : S{
       // Special code to support sealed interfaces encoding as they are not natively supported in kotlinx.serialization
       this.decodeStructure(serializer.descriptor) {
           val unionDecoder = this as UnionDecoder
           val klassName = unionDecoder.decodeString()
           val strategy = serializer.findPolymorphicSerializerInclSealedInterfaceOrNull(this, klassName) ?: throw IllegalStateException("No deserializer found for class name '$klassName'.")
           return unionDecoder.decodeSerializableElement(serializer.descriptor, 1, strategy) as S
       }
    }
}