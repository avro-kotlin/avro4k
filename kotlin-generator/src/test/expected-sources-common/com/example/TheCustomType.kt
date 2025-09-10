package com.example

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

// explicitly not serializable to test custom serializer source generation
class CustomLogicalType {
    class TheNestedSerializer : KSerializer<CustomLogicalType> {
        override val descriptor: SerialDescriptor
            get() = TODO("Not yet implemented")

        override fun serialize(encoder: Encoder, value: CustomLogicalType) {
            TODO("Not yet implemented")
        }

        override fun deserialize(decoder: Decoder): CustomLogicalType {
            TODO("Not yet implemented")
        }
    }
}