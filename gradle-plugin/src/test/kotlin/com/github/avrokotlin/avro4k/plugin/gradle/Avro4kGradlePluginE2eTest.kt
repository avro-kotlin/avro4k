package com.github.avrokotlin.avro4k.plugin.gradle

import io.kotest.assertions.withClue
import io.kotest.matchers.collections.shouldBeIn
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File

class Avro4kGradlePluginE2eTest {
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

    fun BuildResult.shouldHaveTaskSuccessOrUpToDate(taskPath: String) {
        val task = this.task(taskPath)
        withClue("'$taskPath' task should succeed or be up-to-date") {
            task?.outcome shouldBeIn listOf(TaskOutcome.SUCCESS, TaskOutcome.UP_TO_DATE)
        }
    }
}