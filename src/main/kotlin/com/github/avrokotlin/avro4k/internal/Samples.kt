package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.FieldNamingStrategy
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic

@Suppress("KotlinUnreachableCode")
internal object Samples {
    fun customizeAvroInstance() {
        val yourAvroCustomizedInstance =
            Avro {
                fieldNamingStrategy = FieldNamingStrategy.Builtins.SnakeCase
                implicitNulls = false
                implicitEmptyCollections = false
                validateSerialization = true
                serializersModule =
                    SerializersModule {
                        // Register your custom serializers here
                        contextual(YourType::class, YourTypeSerializer())
                        // Set the possible implementations of an abstract class or an interface
                        polymorphic(YourParentType::class) {
                            subclass(YourSubType::class, YourSubTypeSerializer())
                        }
                    }
            }
        yourAvroCustomizedInstance.encodeToByteArray(YourType())
        yourAvroCustomizedInstance.decodeFromByteArray<YourParentType>(TODO("byteArray"))
    }

    class YourType

    class YourTypeSerializer : kotlinx.serialization.KSerializer<YourType> {
        override val descriptor get() = TODO("Not yet implemented")

        override fun serialize(encoder: Encoder, value: YourType) = TODO("Not yet implemented")

        override fun deserialize(decoder: Decoder) = TODO("Not yet implemented")
    }

    interface YourParentType

    object YourSubType : YourParentType

    class YourSubTypeSerializer : kotlinx.serialization.KSerializer<YourSubType> {
        override val descriptor get() = TODO("Not yet implemented")

        override fun serialize(encoder: Encoder, value: YourSubType) = TODO("Not yet implemented")

        override fun deserialize(decoder: Decoder) = TODO("Not yet implemented")
    }
}