package com.github.avrokotlin.avro4k

import io.kotest.matchers.file.shouldHaveSameStructureAndContentAs
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.io.File
import java.nio.file.FileVisitResult
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.Path
import kotlin.io.path.isDirectory
import kotlin.io.path.visitFileTree

class KotlinGeneratorTest {
    @EnabledIfEnvironmentVariable(named = "CI", matches = "true")
    @ParameterizedTest
    @MethodSource("getSchemasToTest")
    fun verifyGeneration(schemaPath: File) {
        // TODO decimal logical type
        val testName = schemaPath.nameWithoutExtension
        val expectedBaseDir = Path("src/test/expected-sources/$testName/").toFile()
        val actualBaseDir = Path("build/generated-sources/$testName/").toFile()

        actualBaseDir.deleteRecursively()
        generateKotlinFiles(testName, schemaPath.readText(), actualBaseDir)

        actualBaseDir shouldHaveSameStructureAndContentAs expectedBaseDir
    }

    @DisabledIfEnvironmentVariable(named = "CI", matches = "true")
    @ParameterizedTest
    @MethodSource("getSchemasToTest")
    fun updateExpectedGeneratedSources(schemaPath: File) {
        val testName = schemaPath.nameWithoutExtension
        val baseDir = Path("src/test/expected-sources/$testName/").toFile()

        baseDir.deleteRecursively()
        generateKotlinFiles(testName, schemaPath.readText(), baseDir)
    }

    companion object {
        @OptIn(ExperimentalPathApi::class)
        @JvmStatic
        fun getSchemasToTest(): List<File> {
            val output = mutableListOf<File>()
            Path("src/test/resources").visitFileTree(maxDepth = 1) {
                onVisitFile { path, _ ->
                    if (!path.isDirectory()) {
                        output += path.toFile()
                    }
                    FileVisitResult.SKIP_SUBTREE
                }
            }
            return output
        }
    }

    private fun generateKotlinFiles(testName: String, schemaContent: String, outputBaseDir: File) {
        val nameStrategy = resolveNameStrategy(testName)
        KotlinGenerator(
            nameStrategy = nameStrategy,
            logicalTypes =
                listOf(
                    KotlinGenerator.LogicalTypeDescriptor(
                        logicalTypeName = "contextualLogicalType",
                        kotlinClassName = "com.example.CustomLogicalType"
                    ),
                    KotlinGenerator.LogicalTypeDescriptor(
                        logicalTypeName = "customLogicalTypeWithKSerializer",
                        kotlinClassName = "com.example.CustomLogicalType",
                        kSerializerClassName = "com.example.CustomLogicalType.TheNestedSerializer"
                    )
                )
        ).generateKotlinClasses(schemaContent, rootAnonymousSchemaName = "TestSchema").forEach { generatedFile ->
            generatedFile
                .toBuilder()
                .indent("    ")
                .build()
                .writeTo(outputBaseDir)
        }
    }

    private fun resolveNameStrategy(testName: String): NameStrategy =
        when (testName) {
            "field_naming_identity" -> NameStrategy.IDENTITY
            "field_naming_snake" -> NameStrategy.SNAKE_CASE
            "field_naming_pascal" -> NameStrategy.PASCAL_CASE

            "field_naming_custom" ->
                NameStrategy.custom("strip-x-prefix-lower-camel") { original ->
                    NameStrategy.CAMEL_CASE.format(original.removePrefix("x_"))
                }

            else -> NameStrategy.IDENTITY
        }
}