package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import com.github.avrokotlin.avro4k.serializer.InstantToMicroSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateTimeSerializer
import com.github.avrokotlin.avro4k.serializer.LocalTimeSerializer
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.nullable
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema

internal class DateSchemaTest : FunSpec({
    listOf(
        LocalDateSerializer to LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)),
        LocalTimeSerializer to LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)),
        LocalDateTimeSerializer to LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
        InstantSerializer to LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
        InstantToMicroSerializer to LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
    ).forEach { (serializer: KSerializer<out Any>, expected) ->
        test("generate date logical type for $serializer") {
            AvroAssertions.assertThat(serializer)
                .generatesSchema(expected)
        }
        test("generate nullable date logical type for $serializer") {
            AvroAssertions.assertThat(serializer.nullable)
                .generatesSchema(expected.nullable)
        }
    }
})