package com.github.avrokotlin.avro4k.plugin.gradle

import com.github.avrokotlin.avro4k.KotlinGenerator
import com.squareup.kotlinpoet.FileSpec
import org.gradle.api.Action
import org.gradle.api.DefaultTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.SetProperty
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
import kotlin.time.ExperimentalTime
import kotlin.uuid.ExperimentalUuidApi

public abstract class Avro4kPluginExtension {
    @get:Nested
    public abstract val sourcesGeneration: Avro4kPluginSourcesGenerationExtension

    public fun sourcesGeneration(action: Action<in Avro4kPluginSourcesGenerationExtension>): Unit =
        action.execute(sourcesGeneration)
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
     * A set of logical types to use in code generation, mapping a logical type name to the class and a potential [kotlinx.serialization.KSerializer].
     * This allows using specific classes for class fields with a given logical type.
     *
     * To create a new logical type, use [logicalType].
     *
     * Any logical type defined in the avro schema that doesn't have a matching entry in this set will be generated as its type (string, int, ...).
     *
     * Example: given the following logical types:
     * ```
     * logicalTypes += logicalType("uuid").asType("kotlin.uuid.Uuid").withSerializer("your.own.UuidSerializer")
     * logicalTypes += logicalType("custom-type").asType("your.FieldType").contextual()
     * ```
     * And the following Avro schema:
     * ```
     * {
     *   "type": "record",
     *   "name": "MyRecord",
     *   "fields": [
     *     {
     *       "name": "uuidField",
     *       "type": {
     *         "type": "string",
     *         "logicalType": "uuid"
     *       }
     *     },
     *     {
     *       "name": "customField",
     *       "type": {
     *         "type": "string",
     *         "logicalType": "custom-type"
     *       }"
     *     }
     *   ]
     * }
     * ```
     * Then the generated `MyRecord` class will have the following fields:
     * ```
     * @Serializable
     * data class MyRecord(
     *   @Serializable(with = UuidSerializer::class)
     *   val uuidField: Uuid,
     *   @Contextual
     *   val customField: FieldType
     * )
     * ```
     */
    @get:Nested
    public val logicalTypes: Avro4kPluginSourcesGenerationLogicalTypesExtension

    public fun logicalTypes(action: Action<in Avro4kPluginSourcesGenerationLogicalTypesExtension>): Unit =
        action.execute(logicalTypes)
}

public interface Avro4kPluginSourcesGenerationLogicalTypesExtension {
    /**
     * Defines a logical type for Avro schema generation.
     * This will initialize a builder to then set its fully qualified class name and optionally a serializer class name.
     *
     * Example of use:
     * ```
     * // defining a serializer at compile-time for better performances
     * logicalTypes += logicalType("uuid").asType("kotlin.uuid.Uuid").withSerializer("your.own.UuidKSerializer")
     *
     * // or at runtime: here the serializer must be provided at runtime using `Avro { serializersModule = SerializersModule { contextual(TimeClass.serializer()) } }`
     * logicalTypes += logicalType("time").asType("your.own.TimeClass").contextual()
     * ```
     *
     * @param logicalTypeName The name of the logical type to be added.
     * @return A builder instance to further configure the logical type.
     */
    public fun register(logicalTypeName: String): LogicalTypeBuilder1 {
        return LogicalTypeBuilder1(logicalTypeName) { registeredMappings.add(it) }
    }

    @ExperimentalTime
    public fun useKotlinTime(vararg onlyFor: String) {
    }

    @ExperimentalUuidApi
    public fun useKotlinUuid() {
        register("uuid").asType("kotlin.uuid.Uuid").withSerializer("cooooom.github.avrokotlin.avro4k.serializer.UuidSerializer")
    }

    public val registeredMappings: SetProperty<LogicalType>
}

@ConsistentCopyVisibility
@JvmRecord
public data class LogicalType internal constructor(
    val logicalTypeName: String,
    val type: String,
    val serializerType: String?,
) : java.io.Serializable

public class LogicalTypeBuilder1 internal constructor(
    private val logicalTypeName: String,
    private val whenBuilt: (LogicalType) -> Unit,
) {
    /**
     * Specifies the type of the being-built logical type, to be used as the class' field type.
     *
     * @param classFullName the fully qualified class name, which must be in the main classpath to compile properly.
     * @return A builder instance to further configure the logical type.
     */
    public fun asType(classFullName: String): LogicalTypeBuilder2 {
        return LogicalTypeBuilder2(
            logicalTypeName = logicalTypeName,
            typeName = classFullName,
            whenBuilt = whenBuilt
        )
    }
}

public class LogicalTypeBuilder2 internal constructor(
    private val logicalTypeName: String,
    private val typeName: String,
    private val whenBuilt: (LogicalType) -> Unit,
) {
    /**
     * Creates the logical type without compile-time serializer.
     * The serializer have to be passed at runtime, as the logical type generated fields will be annotated with `[kotlinx.serialization.Contextual]`:
     *
     * ```
     * Avro {
     *     serializersModule = SerializersModule {
     *         contextual(YourLogicalTypeClass.serializer())
     *     }
     * }
     * ```
     *
     * To specify the serializer at compile-time, use [withSerializer] instead.
     */
    public fun contextual() {
        whenBuilt(LogicalType(logicalTypeName, typeName, null))
    }

    /**
     * Creates the logical type with a serializer set at compile-time for better performances (no lookup at runtime).
     * The logical type generated fields will be annotated with `[kotlinx.serialization.Serializable]` including the given serializer type.
     *
     * To specify the serializer at runtime, use [contextual] instead (for more advanced users).
     *
     * @param serializerTypeName the fully qualified name of the serializer to be used
     */
    public fun withSerializer(serializerTypeName: String) {
        whenBuilt(LogicalType(logicalTypeName, typeName, serializerTypeName))
    }
}

public class Avro4kGradlePlugin : Plugin<Project> {
    override fun apply(project: Project) {
        val extension = project.extensions.create<Avro4kPluginExtension>("avro4k")
        extension.sourcesGeneration.outputDir.convention(project.layout.buildDirectory.dir("generated/sources/avro/main"))
        extension.sourcesGeneration.inputSchemas.convention(project.layout.projectDirectory.dir("src/main/avro"))
        extension.sourcesGeneration.logicalTypes.registeredMappings.convention(emptySet())

        val task =
            project.tasks.register<GenerateKotlinAvroSourcesTask>("generateAvroKotlinSources") {
                group = "build"
                description = "Generates Avro Kotlin source files from avro schemas"

                inputFiles.setFrom(extension.sourcesGeneration.inputSchemas)
                outputDir.set(extension.sourcesGeneration.outputDir)
                logicalTypes.set(extension.sourcesGeneration.logicalTypes.registeredMappings)
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
    public abstract val logicalTypes: SetProperty<LogicalType>

    @TaskAction
    public fun generateKotlinSources() {
        val logicalTypes = logicalTypes.get()
        if (logicalTypes.distinctBy { it.logicalTypeName }.size != logicalTypes.size) {
            val msg = "Duplicate logical type names found. Please ensure having only one entry per logical type name."
            logger.error(msg)
            error(msg)
        }

        val kotlinGenerator =
            KotlinGenerator(
                logicalTypes =
                    logicalTypes.map {
                        KotlinGenerator.LogicalTypeDescriptor(
                            logicalTypeName = it.logicalTypeName,
                            kotlinClassName = it.type,
                            kSerializerClassName = it.serializerType
                        )
                    }
            )
        val outputDir = outputDir.asFile.get()
        val files = getInputAvroSchemaFiles()

        if (files.isEmpty()) {
            logger.error("No files to generate as there are no input schema files. Please configure avro4k.inputFiles accordingly.")
            return
        }

        files.associateWith { file ->
            logger.info("Generating kotlin sources for schema file: ${file.relativeTo(project.projectDir)}")

            val schemaContent = file.readText()
            kotlinGenerator.generateKotlinClasses(schemaContent, file.nameWithoutExtension)
                .also { generatedFiles ->
                    logger.lifecycle(
                        "Schema ${file.relativeTo(project.projectDir)} generated ${generatedFiles.size} class(es):\n  " + generatedFiles.joinToString("\n  ") { it.fullName() }
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

    private fun getInputAvroSchemaFiles(): List<File> =
        inputFiles
            .asFileTree
            .matching { include("**/*.avsc") }
            .files
            .sortedBy { it.relativeTo(project.projectDir).invariantSeparatorsPath }

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
                                originalSchemaLocation.keys.joinToString(", ") { it.relativeTo(project.projectDir).path }
                            }"
                        }
                    }."
            logger.error(message)
            error(message)
        }
    }
}