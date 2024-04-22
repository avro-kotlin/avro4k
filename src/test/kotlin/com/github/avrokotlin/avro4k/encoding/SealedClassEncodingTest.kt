package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.recordWithSchema
import com.github.avrokotlin.avro4k.schema
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable

class SealedClassEncodingTest : StringSpec({
    "encode/decode sealed classes" {
        AvroAssertions.assertThat(ReferencingSealedClass(Operation.Binary.Add(1, 2)))
            .isEncodedAs(record(recordWithSchema(Avro.schema<Operation.Binary.Add>(), 1, 2)))
    }
    "encode/decode nullable sealed classes" {
        AvroAssertions.assertThat(ReferencingNullableSealedClass(Operation.Binary.Add(1, 2)))
            .isEncodedAs(record(recordWithSchema(Avro.schema<Operation.Binary.Add>(), 1, 2)))
        AvroAssertions.assertThat(ReferencingNullableSealedClass(null))
            .isEncodedAs(record(null))
    }
}) {
    @Serializable
    private data class ReferencingSealedClass(
        val notNullable: Operation,
    )

    @Serializable
    private data class ReferencingNullableSealedClass(
        val nullable: Operation?,
    )

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
        sealed class Binary : Operation {
            abstract val left: Int
            abstract val right: Int

            @Serializable
            data class Add(override val left: Int, override val right: Int) : Binary()

            @Serializable
            data class Substract(override val left: Int, override val right: Int) : Binary()
        }
    }
}