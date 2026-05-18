package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.recordWithSchema
import com.github.avrokotlin.avro4k.schema
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

internal class SealedClassEncodingTest : StringSpec({
    "encode/decode sealed classes" {
        AvroAssertions.assertThat(ReferencingSealedClass(Operation.Binary.Add(1, 2)))
            .isEncodedAs(record(recordWithSchema(Avro.schema<Operation.Binary.Add>(), 1, 2)))
        AvroAssertions.assertThat<Operation>(Operation.Binary.Add(1, 2))
            .isEncodedAs(recordWithSchema(Avro.schema<Operation.Binary.Add>(), 1, 2))
    }
    "encode/decode sealed class union with value class primitive, enum and record subtypes" {
        val shapeSchema = Avro.schema<Shape>()
        val colorSchema = shapeSchema.types.first { it.name == "Color" }
        AvroAssertions.assertThat<Shape>(Shape.Label("hello")).isEncodedAs("hello")
        AvroAssertions.assertThat<Shape>(Shape.Color.RED).isEncodedAs(GenericData.get().createEnum("RED", colorSchema))
        AvroAssertions.assertThat<Shape>(Shape.Circle(5)).isEncodedAs(recordWithSchema(Avro.schema<Shape.Circle>(), 5))
    }
    "encode/decode nullable sealed classes" {
        AvroAssertions.assertThat(ReferencingNullableSealedClass(Operation.Binary.Add(1, 2)))
            .isEncodedAs(record(recordWithSchema(Avro.schema<Operation.Binary.Add>(), 1, 2)))
        AvroAssertions.assertThat(ReferencingNullableSealedClass(null))
            .isEncodedAs(record(null))

        AvroAssertions.assertThat<Operation?>(Operation.Binary.Add(1, 2))
            .isEncodedAs(recordWithSchema(Avro.schema<Operation.Binary.Add>(), 1, 2))
        AvroAssertions.assertThat<Operation?>(null)
            .isEncodedAs(null)
    }
    "decode polymorphic type when schema alias matches kotlin serial name" {
        val currentSchema = Avro.schema<Polygon>()
        val trapezoidSchema = currentSchema.types.first { it.name == "Trapezoid" }
        val legacyTrapezoidSchema = Schema.createRecord("LegacyTrapezoid", null, trapezoidSchema.namespace, false, trapezoidSchema.fields.map { Schema.Field(it.name(), it.schema()) })
            .also { it.addAlias("Trapezoid") }
        val legacySchema = Schema.createUnion(currentSchema.types.map { if (it.name == "Trapezoid") legacyTrapezoidSchema else it })

        val encodedBytes = Avro.encodeToByteArray(currentSchema, Avro.serializersModule.serializer<Polygon>(), Polygon.Trapezoid(4))
        val decoded = Avro.decodeFromByteArray(legacySchema, Avro.serializersModule.serializer<Polygon>(), encodedBytes)
        decoded shouldBe Polygon.Trapezoid(4)
    }
    "decode polymorphic type when schema name matches kotlin @AvroAlias" {
        val currentSchema = Avro.schema<Polygon>()
        val hexagonSchema = currentSchema.types.first { it.name == "Hexagon" }
        val legacyHexagonSchema = Schema.createRecord("LegacyHexagon", null, null, false, hexagonSchema.fields.map { Schema.Field(it.name(), it.schema()) })
        val legacySchema = Schema.createUnion(currentSchema.types.map { if (it.name == "Hexagon") legacyHexagonSchema else it })

        val encodedBytes = Avro.encodeToByteArray(currentSchema, Avro.serializersModule.serializer<Polygon>(), Polygon.Hexagon(6))
        val decoded = Avro.decodeFromByteArray(legacySchema, Avro.serializersModule.serializer<Polygon>(), encodedBytes)
        decoded shouldBe Polygon.Hexagon(6)
    }
}) {
    @Serializable
    private sealed interface Polygon {
        @Serializable
        data class Trapezoid(val sides: Int) : Polygon

        @AvroAlias("LegacyHexagon")
        @Serializable
        data class Hexagon(val sides: Int) : Polygon
    }

    @Serializable
    private sealed interface Shape {
        @JvmInline
        @Serializable
        value class Label(val value: String) : Shape

        @Serializable
        enum class Color : Shape { RED, BLUE }

        @Serializable
        data class Circle(val radius: Int) : Shape
    }

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