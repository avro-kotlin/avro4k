package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import com.github.avrokotlin.avro4k.serializer.InstantToMicroSerializer
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import org.apache.avro.Schema
import java.lang.IllegalArgumentException
import java.time.Instant

class ContextualSchemaTest : StringSpec({

    "schema for contextual serializer" {

        val format1 = Avro(
            serializersModule = SerializersModule {
                contextual(InstantSerializer())
            }
        )

        val format2 = Avro(
            serializersModule = SerializersModule {
                contextual(InstantToMicroSerializer())
            }
        )

        val schema1 = format1.schema(Test.serializer())
        val schema2 = format2.schema(Test.serializer())

        shouldThrow<IllegalArgumentException> {
            Avro.default.schema(Test.serializer())
        }
        val expected1 = Schema.Parser().parse(javaClass.getResourceAsStream("/contextual_1.json"))
        schema1 shouldBe expected1
        val expected2 = Schema.Parser().parse(javaClass.getResourceAsStream("/contextual_2.json"))
        schema2 shouldBe expected2
    }
}) {
    @Serializable
    data class Test(
        @Contextual
        val ts: Instant,
        @Contextual
        val withFallback: Int?,
    )
}
