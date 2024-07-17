package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.FieldNamingStrategy
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

internal class FieldNamingStrategySchemaTest : StringSpec({
    "should convert schema with snake_case to camelCase" {
        AvroAssertions.assertThat<Interface>()
            .withConfig { fieldNamingStrategy = FieldNamingStrategy.Builtins.SnakeCase }
            .generatesSchema(Path("/snake_case_schema.json"))
    }

    "should convert schema with PascalCase to camelCase" {
        AvroAssertions.assertThat<Interface>()
            .withConfig { fieldNamingStrategy = FieldNamingStrategy.Builtins.PascalCase }
            .generatesSchema(Path("/pascal_case_schema.json"))
    }
}) {
    @Serializable
    private data class Interface(
        val name: String,
        val ipv4Address: String,
        val ipv4SubnetMask: Int,
        val v: InternetProtocol,
    )

    @Serializable
    private enum class InternetProtocol { IPv4, IPv6 }
}