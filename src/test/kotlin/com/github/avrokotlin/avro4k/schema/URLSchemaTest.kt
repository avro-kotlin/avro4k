package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.internal.nullable
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import java.net.URL

internal class URLSchemaTest : FunSpec({
    test("accept URL as String") {
        AvroAssertions.assertThat<URLTest>()
            .generatesSchema(Schema.create(Schema.Type.STRING))
    }

    test("accept nullable URL as String union") {
        AvroAssertions.assertThat<URLNullableTest>()
            .generatesSchema(Schema.create(Schema.Type.STRING).nullable)
    }
}) {
    @JvmInline
    @Serializable
    private value class URLTest(
        @Contextual val url: URL,
    )

    @JvmInline
    @Serializable
    private value class URLNullableTest(
        @Contextual val url: URL?,
    )
}