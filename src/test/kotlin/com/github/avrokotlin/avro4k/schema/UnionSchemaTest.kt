@file:Suppress("unused")

package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.schema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import kotlin.io.path.Path

class SealedClassSchemaTest : StringSpec({
    "should throw error when no implementation for an abstract class" {
        shouldThrow<AvroSchemaGenerationException> {
            Avro.schema(Operation.Binary.serializer())
        }
    }

    val polymorphicSerializersModule =
        SerializersModule {
            polymorphic(Operation.Binary::class) {
                subclass(Operation.Binary.Add::class)
                subclass(Operation.Binary.Substract::class)
            }
        }
    "schema for sealed hierarchy" {
        AvroAssertions.assertThat<Operation>()
            .withConfig { serializersModule = polymorphicSerializersModule }
            .generatesSchema(Path("/sealed.json"))
    }
    "referencing a sealed hierarchy" {
        AvroAssertions.assertThat<ReferencingSealedClass>()
            .withConfig { serializersModule = polymorphicSerializersModule }
            .generatesSchema(Path("/sealed_referenced.json"))
    }
    "referencing a nullable sealed hierarchy" {
        AvroAssertions.assertThat<ReferencingNullableSealedClass>()
            .withConfig { serializersModule = polymorphicSerializersModule }
            .generatesSchema(Path("/sealed_nullable_referenced.json"))
    }
}) {
    @Serializable
    private sealed interface Operation {
        @Serializable
        object Nullary : Operation

        @Serializable
        sealed class Unary : Operation {
            abstract val value: Int

            @Serializable
            data class Negate(override val value: Int) : Unary()
        }

        @Serializable
        abstract class Binary : Operation {
            abstract val left: Int
            abstract val right: Int

            @Serializable
            data class Add(override val left: Int, override val right: Int) : Binary()

            @Serializable
            data class Substract(override val left: Int, override val right: Int) : Binary()
        }
    }

    @Serializable
    private data class ReferencingSealedClass(
        val notNullable: Operation,
    )

    @Serializable
    private data class ReferencingNullableSealedClass(
        @AvroDefault("null")
        val nullable: Operation?,
    )
}