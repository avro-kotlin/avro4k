package com.github.avrokotlin.avro4k.plugin.gradle

import com.github.avrokotlin.avro4k.KotlinGenerator
import com.squareup.kotlinpoet.FileSpec
import org.gradle.api.Action
import org.gradle.api.DefaultTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.IgnoreEmptyDirectories
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Nested
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.SkipWhenEmpty
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.TaskAction
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.register
import java.io.File

public abstract class Avro4kPluginExtension {
    @get:Nested
    public abstract val sourcesGeneration: Avro4kPluginSourcesGenerationExtension

    public fun sourcesGeneration(action: Action<in Avro4kPluginSourcesGenerationExtension>): Unit = action.execute(sourcesGeneration)
}

public interface Avro4kPluginSourcesGenerationExtension {
    /**
     * Indicates the avro schema files or directories containing schema files.
     * Only files with `.avsc` extension will be processed.
     *
     * By default, this is set to `src/main/avro` directory in the project.
     */
    public val inputSchemas: ConfigurableFileCollection

    /**
     * The output directory where the generated kotlin files will be placed.
     *
     * By default, this is set to `build/generated/sources/avro/main` directory in the project.
     */
    public val outputDir: DirectoryProperty

    /**
     * A map of logical type names to their corresponding fully qualified class names.
     * This allows custom handling of Avro logical types during code generation.
     *
     * Any schema matching a logical type in this map will be represented using the specified class in the generated code, annotated with `@Contextual`.
     * So you must configure accordingly the used [com.github.avrokotlin.avro4k.Avro] instance with [com.github.avrokotlin.avro4k.AvroBuilder.setLogicalTypeSerializer] to handle these types.
     */
    public val logicalTypes: MapProperty<String, String>

    /**
     * convenience method to register the use of `kotlin.uuid.Uuid` for the `uuid` logical type.
     */
    public fun useKotlinUuid() {
        logicalTypes.put("uuid", "kotlin.uuid.Uuid")
    }
}

public class Avro4kGradlePlugin : Plugin<Project> {
    override fun apply(project: Project) {
        val extension = project.extensions.create<Avro4kPluginExtension>("avro4k")
        extension.sourcesGeneration.outputDir.convention(project.layout.buildDirectory.dir("generated/sources/avro/main"))
        extension.sourcesGeneration.inputSchemas.convention(project.layout.projectDirectory.dir("src/main/avro"))
        extension.sourcesGeneration.logicalTypes.convention(emptyMap())

        val task =
            project.tasks.register<GenerateKotlinAvroSourcesTask>("generateAvroKotlinSources") {
                group = "build"
                description = "Generates Avro Kotlin source files from avro schemas"

                inputFiles.setFrom(extension.sourcesGeneration.inputSchemas)
                outputDir.set(extension.sourcesGeneration.outputDir)
                logicalTypes.set(extension.sourcesGeneration.logicalTypes)
            }

        project.plugins.withId("org.jetbrains.kotlin.jvm") {
            project.extensions.configure<SourceSetContainer>("sourceSets") {
                val generatedSourcesDir = task.map { it.outputDir }

                // make main source set seeing the generated classes
                val mainSourceSet = getByName(SourceSet.MAIN_SOURCE_SET_NAME)
                mainSourceSet.java.srcDirs(project.files(generatedSourcesDir).builtBy(task))

                // simplify dependency management by adding avro4k dependencies, and enforcing consistent versions across the plugin version and the library itself
                val mainConfiguration = project.configurations.getByName(mainSourceSet.implementationConfigurationName)
                project.dependencies {
                    mainConfiguration(enforcedPlatform("com.github.avro-kotlin.avro4k:avro4k-bom:${BuildConfig.AVRO4K_VERSION}"))
                    mainConfiguration("com.github.avro-kotlin.avro4k:avro4k-core")
                }
            }
        }
    }
}

@CacheableTask
public abstract class GenerateKotlinAvroSourcesTask : DefaultTask() {
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    @get:SkipWhenEmpty
    @get:IgnoreEmptyDirectories
    public abstract val inputFiles: ConfigurableFileCollection

    @get:OutputDirectory
    public abstract val outputDir: DirectoryProperty

    @get:Input
    public abstract val logicalTypes: MapProperty<String, String>

    @TaskAction
    public fun generateKotlinSources() {
        val logicalTypeDescriptors = logicalTypes.get().map { (logicalTypeName, kotlinClassName) ->
            KotlinGenerator.LogicalTypeDescriptor(
                logicalTypeName,
                kotlinClassName,
                getWellKnownSerializer(logicalTypeName, kotlinClassName),
            )
        }
        val kotlinGenerator =
            KotlinGenerator(
                logicalTypes = logicalTypeDescriptors
            )
        val outputDir = outputDir.asFile.get()
        val files = getInputAvroSchemaFiles()

        if (files.isEmpty()) {
            logger.error("No files to generate as there are no input schema files. Please configure avro4k.inputFiles accordingly.")
            return
        }

        files.associateWith { file ->
            logger.info("Generating kotlin sources for schema file: $file")

            val schemaContent = file.readText()
            kotlinGenerator.generateKotlinClasses(schemaContent, file.nameWithoutExtension)
                .also { generatedFiles ->
                    logger.lifecycle(
                        "Schema $file generated ${generatedFiles.size} class(es):\n " + generatedFiles.joinToString("\n ") { it.fullName() }
                    )
                }
        }
            .also { it.verifyNoDuplicates() }
            .values.flatten().forEach { generatedFile ->
                generatedFile
                    .toBuilder()
                    .indent("    ")
                    .build()
                    .writeTo(outputDir)
            }
    }

    private fun getWellKnownSerializer(logicalTypeName: String, className: String): String? {
        return when {
            logicalTypeName == "uuid" && className == "kotlin.uuid.Uuid"
                -> "com.github.avrokotlin.avro4k.serializer.KotlinUuidSerializer"
            logicalTypeName == "uuid" && className == "java.util.UUID"
                -> "com.github.avrokotlin.avro4k.serializer.UUIDSerializer"
            else -> null // @Contextual
        }
    }

    private fun getInputAvroSchemaFiles(): List<File> =
        inputFiles
            .asFileTree
            .matching { include("**/*.avsc") }
            .files
            .sorted()

    private fun FileSpec.fullName() = if (packageName.isEmpty()) name else "$packageName.$name"

    private fun Map<File, List<FileSpec>>.verifyNoDuplicates() {
        // links generated class names to the schema files that generated them. If the set has more than one entry, we have a duplicate
        val names = mutableMapOf<String, MutableMap<File, FileSpec>>()
        forEach { (file, generatedFiles) ->
            generatedFiles.forEach { generatedFile ->
                names.getOrPut(generatedFile.fullName()) { mutableMapOf() }[file] = generatedFile
            }
        }

        // Keep only duplicates. Allow generating identical classes from different schema files
        names.entries.removeIf { it.value.size < 2 || it.value.values.toSet().size < 2 }

        if (names.isNotEmpty()) {
            val message =
                "Duplicate generated class names found. " +
                        "This usually indicates that the same schema is defined across multiple avsc files. " +
                        "Please check your input schema files:\n${
                            names.entries.joinToString("\n  ") { (generatedFullName, originalSchemaLocation) ->
                                "$generatedFullName has been generated differently from the following schemas: ${
                                    originalSchemaLocation.keys.joinToString(", ") { it.path }
                                }"
                            }
                        }."
            logger.error(message)
            error(message)
        }
    }
}