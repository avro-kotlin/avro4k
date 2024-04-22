package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import java.time.Instant
import kotlin.io.path.Path

class ContextualSchemaTest : StringSpec({

    "schema for contextual serializer" {
        AvroAssertions.assertThat<Test>()
            .withConfig { serializersModule = SerializersModule { contextual(MySerializer) } }
            .generatesSchema(Path("/contextual_1.json"))
    }
}) {
    @Serializable
    private data class Test(
        @Contextual
        val ts: Instant,
        @Contextual
        val withFallback: Int?,
    )

    private object MySerializer : KSerializer<Instant> {
        override val descriptor = PrimitiveSerialDescriptor("MySerializer", PrimitiveKind.BOOLEAN)

        override fun deserialize(decoder: Decoder): Instant {
            TODO("Not yet implemented")
        }

        override fun serialize(
            encoder: Encoder,
            value: Instant,
        ) {
            TODO("Not yet implemented")
        }
    }
}