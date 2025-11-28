package com.github.avrokotlin.avro4k.plugin.gradle

import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class Avro4kGradlePluginIntegrationTest {
    @field:TempDir
    private lateinit var projectDir: File

    private val settingsFile by lazy { projectDir.resolve("settings.gradle.kts") }
    private val buildFile by lazy { projectDir.resolve("build.gradle.kts") }
    private val schemaFile by lazy { projectDir.resolve("schema.avsc") }
    private val generatedSourceFile by lazy { projectDir.resolve("build/generated/sources/avro/main/Schema.kt") }

    @Test
    fun `sources generation can be configured with custom logical types`() {
        settingsFile.writeText("")
        schemaFile.writeText(
            """
            {
                "type" : "record",
                "name" : "Schema",
                "fields" : [
                    {
                        "name" : "uuid",
                        "type" : {"type":"string", "logicalType":"uuid"}
                    },
                    {
                        "name" : "customType",
                        "type" : {"type":"string", "logicalType":"custom-logical-type"}
                    },
                    {
                        "name" : "ignoredType",
                        "type" : {"type":"string", "logicalType":"another-logical-type"}
                    }
                ]
            }
            """.trimIndent()
        )
        buildFile.writeText(
            """
            plugins {
                id("io.github.avro-kotlin")
            }
            avro4k {
                sourcesGeneration {
                    inputSchemas.from(file("schema.avsc"))
                    logicalTypes {
                        register("uuid").asType("kotlin.uuid.Uuid", "your.own.CustomUuidKSerializer")
                        register("custom-logical-type").asContextualType("your.OwnType")
                        
                        // Partial mappings are ignored as incomplete
                        register("another-logical-type")
                    }
                }
            }
            """.trimIndent()
        )

        // Run build
        val result =
            GradleRunner.create()
                .withProjectDir(projectDir)
                .withPluginClasspath()
                .withArguments("generateAvroKotlinSources", "--stacktrace")
                .forwardOutput()
                .withDebug(true)
                .build()!!

        result.shouldHaveTaskSuccess(":generateAvroKotlinSources")

        // Check the generated file contains our custom type. For deeper tests, all is done in kotlin-generator module
        val generatedContent = generatedSourceFile.readText()
        println("Generated file:\n$generatedContent")
        generatedContent shouldContain "kotlin.uuid.Uuid"
        generatedContent shouldContain "your.OwnType"
        generatedContent shouldContain "kotlinx.serialization.Contextual"
        generatedContent shouldContain "your.own.CustomUuidKSerializer"
        generatedContent shouldContain "ignoredType: String"
    }

    fun BuildResult.shouldHaveTaskSuccess(taskPath: String) {
        val task = this.task(taskPath)
        withClue("'$taskPath' task should succeed") {
            task?.outcome shouldBe TaskOutcome.SUCCESS
        }
    }
}