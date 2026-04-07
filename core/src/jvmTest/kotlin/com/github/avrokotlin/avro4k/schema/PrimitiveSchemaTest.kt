package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.WrappedBoolean
import com.github.avrokotlin.avro4k.WrappedByte
import com.github.avrokotlin.avro4k.WrappedChar
import com.github.avrokotlin.avro4k.WrappedDouble
import com.github.avrokotlin.avro4k.WrappedFloat
import com.github.avrokotlin.avro4k.WrappedInt
import com.github.avrokotlin.avro4k.WrappedLong
import com.github.avrokotlin.avro4k.WrappedShort
import com.github.avrokotlin.avro4k.WrappedString
import com.github.avrokotlin.avro4k.internal.nullable
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.serializer
import org.apache.avro.LogicalType
import org.apache.avro.Schema

@OptIn(InternalSerializationApi::class)
internal class PrimitiveSchemaTest : StringSpec({
    listOf(
        WrappedBoolean::class to Schema.create(Schema.Type.BOOLEAN),
        WrappedByte::class to Schema.create(Schema.Type.INT),
        WrappedShort::class to Schema.create(Schema.Type.INT),
        WrappedInt::class to Schema.create(Schema.Type.INT),
        WrappedLong::class to Schema.create(Schema.Type.LONG),
        WrappedFloat::class to Schema.create(Schema.Type.FLOAT),
        WrappedDouble::class to Schema.create(Schema.Type.DOUBLE),
        WrappedString::class to Schema.create(Schema.Type.STRING),
        WrappedChar::class to Schema.create(Schema.Type.INT).also { LogicalType("char").addToSchema(it) }
    ).forEach { (type, expectedSchema) ->
        "value class ${type.simpleName} should be primitive schema $expectedSchema" {
            AvroAssertions.assertThat(type.serializer())
                .generatesSchema(expectedSchema)
        }
    }

    listOf(
        Boolean::class to Schema.create(Schema.Type.BOOLEAN),
        Byte::class to Schema.create(Schema.Type.INT),
        Short::class to Schema.create(Schema.Type.INT),
        Int::class to Schema.create(Schema.Type.INT),
        Long::class to Schema.create(Schema.Type.LONG),
        Float::class to Schema.create(Schema.Type.FLOAT),
        Double::class to Schema.create(Schema.Type.DOUBLE),
        String::class to Schema.create(Schema.Type.STRING),
        Char::class to Schema.create(Schema.Type.INT).also { LogicalType("char").addToSchema(it) }
    ).forEach { (type, expectedSchema) ->
        "type ${type.simpleName} should be primitive schema $expectedSchema" {
            AvroAssertions.assertThat(type.serializer())
                .generatesSchema(expectedSchema)
        }
    }

    listOf(
        Boolean::class to Schema.create(Schema.Type.BOOLEAN),
        Byte::class to Schema.create(Schema.Type.INT),
        Short::class to Schema.create(Schema.Type.INT),
        Int::class to Schema.create(Schema.Type.INT),
        Long::class to Schema.create(Schema.Type.LONG),
        Float::class to Schema.create(Schema.Type.FLOAT),
        Double::class to Schema.create(Schema.Type.DOUBLE),
        String::class to Schema.create(Schema.Type.STRING),
        Char::class to Schema.create(Schema.Type.INT).also { LogicalType("char").addToSchema(it) }
    ).forEach { (type, expectedSchema) ->
        "type ${type.simpleName}? should be nullable primitive schema $expectedSchema" {
            AvroAssertions.assertThat(type.serializer().nullable)
                .generatesSchema(expectedSchema.nullable)
        }
    }
})