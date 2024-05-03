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
import com.github.avrokotlin.avro4k.nullable
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.serializer
import org.apache.avro.Schema

@OptIn(InternalSerializationApi::class)
internal class PrimitiveSchemaTest : StringSpec({
    listOf(
        WrappedBoolean::class to Schema.Type.BOOLEAN,
        WrappedByte::class to Schema.Type.INT,
        WrappedShort::class to Schema.Type.INT,
        WrappedInt::class to Schema.Type.INT,
        WrappedLong::class to Schema.Type.LONG,
        WrappedFloat::class to Schema.Type.FLOAT,
        WrappedDouble::class to Schema.Type.DOUBLE,
        WrappedChar::class to Schema.Type.INT,
        WrappedString::class to Schema.Type.STRING
    ).forEach { (type, expectedType) ->
        "value class ${type.simpleName} should be primitive schema $expectedType" {
            AvroAssertions.assertThat(type.serializer())
                .generatesSchema(Schema.create(expectedType))
        }
    }

    listOf(
        Boolean::class to Schema.Type.BOOLEAN,
        Byte::class to Schema.Type.INT,
        Short::class to Schema.Type.INT,
        Int::class to Schema.Type.INT,
        Long::class to Schema.Type.LONG,
        Float::class to Schema.Type.FLOAT,
        Double::class to Schema.Type.DOUBLE,
        Char::class to Schema.Type.INT,
        String::class to Schema.Type.STRING
    ).forEach { (type, expectedType) ->
        "type ${type.simpleName} should be primitive schema $expectedType" {
            AvroAssertions.assertThat(type.serializer())
                .generatesSchema(Schema.create(expectedType))
        }
    }

    listOf(
        Boolean::class to Schema.Type.BOOLEAN,
        Byte::class to Schema.Type.INT,
        Short::class to Schema.Type.INT,
        Int::class to Schema.Type.INT,
        Long::class to Schema.Type.LONG,
        Float::class to Schema.Type.FLOAT,
        Double::class to Schema.Type.DOUBLE,
        Char::class to Schema.Type.INT,
        String::class to Schema.Type.STRING
    ).forEach { (type, expectedType) ->
        "type ${type.simpleName}? should be nullable primitive schema $expectedType" {
            AvroAssertions.assertThat(type.serializer().nullable)
                .generatesSchema(Schema.create(expectedType).nullable)
        }
    }
})