package com.github.avrokotlin.avro4k.plugin.gradle

import io.kotest.assertions.withClue
import io.kotest.matchers.collections.shouldBeIn
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.io.path.createTempDirectory

class Avro4kGradlePluginTest {
    @Test
    fun `plugin generates kotlin classes and compiles`() {
        val projectDir = File("../example/kotlin-plugin").canonicalFile
        File(projectDir, "build").deleteRecursively()

        // Run build
        val result =
            GradleRunner.create()
                .withProjectDir(projectDir)
                .withArguments("compileKotlin")
                .withPluginClasspath() // this adds gradle-plugin to classpath
                .forwardOutput()
                .withDebug(true)
                .build()!!

        // compileKotlin should generate avro sources, and also compile them
        result.shouldHaveTaskSuccessOrUpToDate(":generateAvroKotlinSources")
        result.shouldHaveTaskSuccessOrUpToDate(":compileKotlin")

        // Check generated file exists
        assertTrue(File(projectDir, "build/generated/sources/avro/main/MyKeySchema.kt").exists())
        assertTrue(File(projectDir, "build/generated/sources/avro/main/example/avro4k/MyValueSchema.kt").exists())
    }

    @Test
    fun `plugin generates UUID with java util UUID by default`() {
        val projectDir = File("../example/kotlin-plugin").canonicalFile
        File(projectDir, "build").deleteRecursively()

        // Run build
        GradleRunner.create()
            .withProjectDir(projectDir)
            .withArguments("generateAvroKotlinSources")
            .withPluginClasspath()
            .forwardOutput()
            .withDebug(true)
            .build()!!

        // Check generated file contains java.util.UUID
        val generatedFile = File(projectDir, "build/generated/sources/avro/main/MyKeySchema.kt")
        assertTrue(generatedFile.exists())
        val content = generatedFile.readText()
        content shouldContain "import java.util.UUID"
        content shouldContain "import com.github.avrokotlin.avro4k.serializer.UUIDSerializer"
        content shouldContain "public val `value`: UUID"
    }

    @Test
    fun `useKotlinUuid generates code with kotlin native UUID`() {
        val projectDir = createTempDirectory(prefix = "avro4k-test-").toFile()

        try {
            // Create test project structure
            setupTestProject(projectDir, useKotlinUuid = true)

            // Run build
            val result = GradleRunner.create()
                .withProjectDir(projectDir)
                .withArguments("generateAvroKotlinSources", "--stacktrace")
                .withPluginClasspath()
                .forwardOutput()
                .withDebug(true)
                .build()!!

            result.shouldHaveTaskSuccessOrUpToDate(":generateAvroKotlinSources")

            // Check generated file contains kotlin.uuid.Uuid
            val generatedFile = File(projectDir, "build/generated/sources/avro/main/TestUuidSchema.kt")
            assertTrue(generatedFile.exists(), "Generated file should exist")

            val content = generatedFile.readText()
            content shouldContain "import kotlin.uuid.Uuid"
            content shouldContain "import com.github.avrokotlin.avro4k.serializer.KotlinUuidSerializer"
            content shouldContain "@Serializable(with = KotlinUuidSerializer::class)"
            content shouldContain "public val `value`: Uuid"

            // Should not contain java.util.UUID
            content shouldNotContain "java.util.UUID"
            content shouldNotContain "UUIDSerializer"
        } finally {
            projectDir.deleteRecursively()
        }
    }

    @Test
    fun `logicalTypes map can be configured directly`() {
        val projectDir = createTempDirectory(prefix = "avro4k-test-").toFile()

        try {
            // Create test project with direct logicalTypes configuration
            setupTestProject(projectDir, useDirectConfig = true)

            // Run build
            val result = GradleRunner.create()
                .withProjectDir(projectDir)
                .withArguments("generateAvroKotlinSources", "--stacktrace")
                .withPluginClasspath()
                .forwardOutput()
                .withDebug(true)
                .build()!!

            result.shouldHaveTaskSuccessOrUpToDate(":generateAvroKotlinSources")

            // Check generated file contains kotlin.uuid.Uuid
            val generatedFile = File(projectDir, "build/generated/sources/avro/main/TestUuidSchema.kt")
            assertTrue(generatedFile.exists(), "Generated file should exist")

            val content = generatedFile.readText()
            content shouldContain "kotlin.uuid.Uuid"
            content shouldContain "KotlinUuidSerializer"
        } finally {
            projectDir.deleteRecursively()
        }
    }

    private fun setupTestProject(
        projectDir: File,
        useKotlinUuid: Boolean = false,
        useDirectConfig: Boolean = false
    ) {
        // Create directories
        File(projectDir, "src/main/avro").mkdirs()

        // Create UUID schema
        File(projectDir, "src/main/avro/test-uuid-schema.avsc").writeText(
            """
            {
                "type": "string",
                "logicalType": "uuid"
            }
            """.trimIndent()
        )

        // Create build.gradle.kts
        val buildScript = when {
            useKotlinUuid -> """
                plugins {
                    kotlin("jvm") version "2.2.10"
                    id("io.github.avro-kotlin")
                }

                repositories {
                    mavenCentral()
                    mavenLocal()
                }

                avro4k {
                    sourcesGeneration {
                        useKotlinUuid()
                    }
                }
            """.trimIndent()

            useDirectConfig -> """
                plugins {
                    kotlin("jvm") version "2.2.10"
                    id("io.github.avro-kotlin")
                }

                repositories {
                    mavenCentral()
                    mavenLocal()
                }

                avro4k {
                    sourcesGeneration {
                        logicalTypes.put("uuid", "kotlin.uuid.Uuid")
                    }
                }
            """.trimIndent()

            else -> """
                plugins {
                    kotlin("jvm") version "2.2.10"
                    id("io.github.avro-kotlin")
                }

                repositories {
                    mavenCentral()
                    mavenLocal()
                }
            """.trimIndent()
        }

        File(projectDir, "build.gradle.kts").writeText(buildScript)

        // Create settings.gradle.kts
        File(projectDir, "settings.gradle.kts").writeText(
            """
            rootProject.name = "test-project"
            """.trimIndent()
        )
    }

    fun BuildResult.shouldHaveTaskSuccessOrUpToDate(taskPath: String) {
        val task = this.task(taskPath)
        withClue("'$taskPath' task should succeed or be up-to-date") {
            task?.outcome shouldBeIn listOf(TaskOutcome.SUCCESS, TaskOutcome.UP_TO_DATE)
        }
    }
}