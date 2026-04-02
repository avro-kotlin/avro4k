package com.github.avrokotlin.avro4k

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.file.shouldHaveSameStructureAndContentAs
import org.apache.avro.Schema
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.io.File
import java.nio.file.FileVisitResult
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.Path
import kotlin.io.path.visitFileTree

class KotlinGeneratorTest {
    @EnabledIfEnvironmentVariable(named = "CI", matches = "true")
    @ParameterizedTest
    @MethodSource("getSchemasToTest")
    fun verifyGeneration(schemaPath: File) {
        val testName = schemaPath.nameWithoutExtension
        val expectedBaseDir = File("src/test/expected-sources/$testName/")
        val actualBaseDir = File("build/generated-sources/$testName/")

        if (testName.endsWith("-fail")) {
            shouldThrow<Exception> {
                generateKotlinFiles(schemaPath, actualBaseDir)
            }
        } else {
            generateKotlinFiles(schemaPath, actualBaseDir)

            actualBaseDir shouldHaveSameStructureAndContentAs expectedBaseDir
        }
    }

    @DisabledIfEnvironmentVariable(named = "CI", matches = "true")
    @ParameterizedTest
    @MethodSource("getSchemasToTest")
    fun updateExpectedGeneratedSources(schemaPath: File) {
        val testName = schemaPath.nameWithoutExtension
        val baseDir = File("src/test/expected-sources/$testName/")

        if (testName.endsWith("-fail")) {
            shouldThrow<Exception> {
                generateKotlinFiles(schemaPath, baseDir)
            }
        } else {
            generateKotlinFiles(schemaPath, baseDir)
        }
    }

    companion object {
        @OptIn(ExperimentalPathApi::class)
        @JvmStatic
        fun getSchemasToTest(): List<File> {
            val output = mutableListOf<File>()
            Path("src/test/resources").visitFileTree(maxDepth = 1) {
                onVisitFile { path, _ ->
                    output += path.toFile()
                    FileVisitResult.SKIP_SUBTREE
                }
            }
            return output
        }
    }

    private fun generateKotlinFiles(schemaFile: File, outputBaseDir: File) {
        val testName = schemaFile.nameWithoutExtension
        val fieldNamingStrategy = resolveFieldNamingStrategy(testName)
        outputBaseDir.deleteRecursively()
        val generator =
            KotlinGenerator(
                fieldNamingStrategy = fieldNamingStrategy,
                additionalLogicalTypes =
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
            )
        if (schemaFile.isDirectory) {
            generator.generateKotlinClassesFromFiles(schemaFile.listFiles()!!.toList()).flatMap { it.second }
        } else {
            generator.generateKotlinClasses(Schema.Parser().parse(schemaFile), rootAnonymousSchemaName = "TestSchema")
        }.forEach { generatedFile ->
            generatedFile
                .toBuilder()
                .indent("    ")
                .build()
                .writeTo(outputBaseDir)
        }
    }

    private fun resolveFieldNamingStrategy(testName: String): FieldNamingStrategy =
        when {
            testName.startsWith("field-naming-camel-case") -> FieldNamingStrategy.CamelCase
            else -> FieldNamingStrategy.Identity
        }
}