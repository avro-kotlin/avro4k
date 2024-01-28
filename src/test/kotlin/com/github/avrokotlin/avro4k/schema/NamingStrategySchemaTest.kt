package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroConfiguration
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class NamingStrategySchemaTest : WordSpec({
    "NamingStrategy" should {
        "convert schema with snake_case to camelCase" {
            val snakeCaseAvro = Avro(AvroConfiguration(fieldNamingStrategy = FieldNamingStrategy.SnakeCase))

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/snake_case_schema.json"))

            val schema = snakeCaseAvro.schema(Interface.serializer())

            schema.toString(true) shouldBe expected.toString(true)
        }

        "convert schema with PascalCase to camelCase" {
            val pascalCaseAvro = Avro(AvroConfiguration(fieldNamingStrategy = FieldNamingStrategy.PascalCase))

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/pascal_case_schema.json"))

            val schema = pascalCaseAvro.schema(Interface.serializer())

            schema.toString(true) shouldBe expected.toString(true)
        }
    }
}) {
    @Serializable
    data class SubInterface(val name: String, val ipv4Address: String)

    @Serializable
    data class Interface(
        val name: String,
        val ipv4Address: String,
        val ipv4SubnetMask: Int,
        val v: InternetProtocol,
        val subInterface: SubInterface?,
    )
}

@Serializable
enum class InternetProtocol { IPv4, IPv6 }