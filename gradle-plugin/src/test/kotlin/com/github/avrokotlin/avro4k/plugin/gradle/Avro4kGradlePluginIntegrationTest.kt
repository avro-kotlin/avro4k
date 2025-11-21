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
    fun `plugin generates kotlin classes and compiles`() {
        settingsFile.writeText("")
        schemaFile.writeText("""{"type":"string", "logicalType":"uuid"}""")
        buildFile.writeText(
            """
            plugins {
                id("io.github.avro-kotlin")
            }
            avro4k {
                sourcesGeneration {
                    inputSchemas.from(file("schema.avsc"))
                    logicalTypes {
                        @OptIn(kotlin.time.ExperimentalTime::class)
                        useKotlinTime()
                        @OptIn(kotlin.uuid.ExperimentalUuidApi::class)
                        useKotlinUuid()
            
                        register("cutom-type").asType("your.OwnType").withSerializer("your.own.CustomKSerializer")
                        register("custom-logical-type").asType("your.OwnType").contextual()
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

        // Check the generated file exists
        generatedSourceFile.readText().also { println("Genrated file:\n$it") } shouldContain "kotlin.uuid.Uuid"
    }

    fun BuildResult.shouldHaveTaskSuccess(taskPath: String) {
        val task = this.task(taskPath)
        withClue("'$taskPath' task should succeed") {
            task?.outcome shouldBe TaskOutcome.SUCCESS
        }
    }
}