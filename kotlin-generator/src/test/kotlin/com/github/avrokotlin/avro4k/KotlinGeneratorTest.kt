package com.github.avrokotlin.avro4k

import io.kotest.matchers.file.shouldHaveSameStructureAndContentAs
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.io.File
import java.nio.file.FileVisitResult
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.Path
import kotlin.io.path.isDirectory
import kotlin.io.path.visitFileTree

class KotlinGeneratorTest {
    @ParameterizedTest
    @MethodSource("getSchemasToTest")
    fun verifyGeneration(schemaPath: File) {
        val testName = schemaPath.nameWithoutExtension
        val expectedBaseDir = Path("src/test/expected-sources/$testName/").toFile()
        val actualBaseDir = Path("build/generated-sources/$testName/").toFile()

        actualBaseDir.deleteRecursively()
        generateKotlinFiles(schemaPath.readText(), actualBaseDir)

        actualBaseDir shouldHaveSameStructureAndContentAs expectedBaseDir
    }

//    @Disabled
    @ParameterizedTest
    @MethodSource("getSchemasToTest")
    fun updateExpectedGeneratedSources(schemaPath: File) {
        val testName = schemaPath.nameWithoutExtension
        val baseDir = Path("src/test/expected-sources/$testName/").toFile()

        baseDir.deleteRecursively()
        generateKotlinFiles(schemaPath.readText(), baseDir)
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

    private fun generateKotlinFiles(schemaContent: String, outputBaseDir: File) {
        KotlinGenerator().generateKotlinClasses(schemaContent, rootAnonymousSchemaName = "TestSchema").forEach { generatedFile ->
            generatedFile
                .toBuilder()
                .indent("    ")
                .build()
                .writeTo(outputBaseDir)
        }
    }
}