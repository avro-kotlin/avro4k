package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.asAvroLogicalType
import com.github.avrokotlin.avro4k.internal.asAvroLogicalType
import com.github.avrokotlin.avro4k.nullable
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.nullable
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.LogicalType
import org.apache.avro.Schema

internal class AvroLogicalTypeTest : StringSpec({
    "support custom logical type with lambda providing the logical type" {
        AvroAssertions.assertThat<CustomLogicalTypeUsingLambda>()
            .generatesSchema(CustomLogicalType.addToSchema(Schema.create(Schema.Type.STRING)))
    }
    "support custom logical type providing the logical type using serial name" {
        AvroAssertions.assertThat<CustomLogicalTypeUsingSerialName>()
            .generatesSchema(CustomLogicalType.addToSchema(Schema.create(Schema.Type.STRING)))
        AvroAssertions.assertThat<CustomLogicalTypeUsingSerialNameFieldNullable>()
            .generatesSchema(CustomLogicalType.addToSchema(Schema.create(Schema.Type.STRING)).nullable)
        AvroAssertions.assertThat<CustomLogicalTypeUsingSerialNameDescriptorNullable>()
            .generatesSchema(CustomLogicalType.addToSchema(Schema.create(Schema.Type.STRING)).nullable)
    }
}) {
    @JvmInline
    @Serializable
    private value class CustomLogicalTypeUsingLambda(
        @Serializable(with = CustomLogicalTypeSerializer::class) val value: String,
    )

    private object CustomLogicalType : LogicalType("custom")

    private object CustomLogicalTypeSerializer : KSerializer<String> {
        override val descriptor: SerialDescriptor
            get() =
                PrimitiveSerialDescriptor("CustomLogicalType", PrimitiveKind.STRING)
                    .asAvroLogicalType { CustomLogicalType }

        override fun deserialize(decoder: Decoder): String {
            TODO("Not yet implemented")
        }

        override fun serialize(
            encoder: Encoder,
            value: String,
        ) {
            TODO("Not yet implemented")
        }
    }

    @JvmInline
    @Serializable
    private value class CustomLogicalTypeUsingSerialName(
        @Serializable(with = CustomLogicalTypeUsingSerialNameSerializer::class) val value: String,
    )

    @JvmInline
    @Serializable
    private value class CustomLogicalTypeUsingSerialNameFieldNullable(
        @Serializable(with = CustomLogicalTypeUsingSerialNameSerializer::class) val value: String?,
    )

    @JvmInline
    @Serializable
    private value class CustomLogicalTypeUsingSerialNameDescriptorNullable(
        @Serializable(with = CustomLogicalTypeUsingSerialNameNullableSerializer::class) val value: String?,
    )

    private object CustomLogicalTypeUsingSerialNameSerializer : KSerializer<String> {
        override val descriptor: SerialDescriptor
            get() =
                PrimitiveSerialDescriptor("custom", PrimitiveKind.STRING)
                    .asAvroLogicalType()

        override fun deserialize(decoder: Decoder): String {
            TODO("Not yet implemented")
        }

        override fun serialize(
            encoder: Encoder,
            value: String,
        ) {
            TODO("Not yet implemented")
        }
    }

    private object CustomLogicalTypeUsingSerialNameNullableSerializer : KSerializer<String?> {
        override val descriptor: SerialDescriptor
            get() =
                PrimitiveSerialDescriptor("custom", PrimitiveKind.STRING)
                    .nullable
                    .asAvroLogicalType()

        override fun deserialize(decoder: Decoder): String? {
            TODO("Not yet implemented")
        }

        override fun serialize(
            encoder: Encoder,
            value: String?,
        ) {
            TODO("Not yet implemented")
        }
    }
}