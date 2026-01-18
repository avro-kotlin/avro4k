package com.github.avrokotlin.avro4k.plugin.gradle

import com.github.avrokotlin.avro4k.FieldNamingStrategy
import com.github.avrokotlin.avro4k.KotlinGenerator
import com.github.avrokotlin.avro4k.generateKotlinClassesFromFiles
import com.squareup.kotlinpoet.FileSpec
import org.gradle.api.Action
import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.IgnoreEmptyDirectories
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Nested
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.SkipWhenEmpty
import org.gradle.api.tasks.TaskAction
import java.io.File

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
     * When true, uses Kotlin naming conventions for generated field names (camelCase).
     * Defaults to false, keeping original avro field names.
     */
    public val useKotlinConventionForFieldNames: Property<Boolean>

    @get:Nested
    public val logicalTypes: Avro4kPluginSourcesGenerationLogicalTypesExtension

    /**
     * Configures the logical types to use in code generation, mapping a logical type name to the class and its serializer (if not contextual).
     * This allows using specific classes for class fields with a given logical type.
     *
     * Any logical type defined in the avro schema that doesn't have a matching entry in this set will be generated as its core type (string, int, ...).
     *
     * Example: given the following logical types:
     * ```
     * avro4k {
     *   sourcesGeneration {
     *     logicalTypes {
     *       register("uuid").asType("kotlin.uuid.Uuid", "your.own.UuidSerializer")
     *       register("custom-type").asContextualType("your.FieldType")
     *     }
     *   }
     * }
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
    public fun logicalTypes(action: Action<in Avro4kPluginSourcesGenerationLogicalTypesExtension>): Unit =
        action.execute(logicalTypes)
}

public interface Avro4kPluginSourcesGenerationLogicalTypesExtension {
    public fun register(logicalTypeName: String): LogicalTypeBuilder {
        return LogicalTypeBuilder(logicalTypeName) { k, mapping -> mappings.put(k, mapping) }
    }

    @get:Input
    public val mappings: MapProperty<String, LogicalTypeMapping>
}

@ConsistentCopyVisibility
@JvmRecord
public data class LogicalTypeMapping internal constructor(
    val type: String,
    val serializerType: String?,
) : java.io.Serializable

public class LogicalTypeBuilder internal constructor(
    private val logicalTypeName: String,
    private val whenBuilt: (String, LogicalTypeMapping) -> Unit,
) {
    /**
     * Maps the logical type to a given class with a serializer set at compile-time for better performances (no lookup at runtime).
     * The fields generated from a logical type will be annotated with `@kotlinx.serialization.Serializable(with = serializerTypeName)`.
     *
     * To specify the serializer at runtime, use [asContextualType] instead (for more advanced users).
     *
     * @param serializerTypeName the fully qualified name of the serializer to be used
     */
    public fun asType(className: String, serializerTypeName: String) {
        whenBuilt(logicalTypeName, LogicalTypeMapping(className, serializerTypeName))
    }

    /**
     * Creates the logical type without compile-time serializer.
     * The serializer have to be passed at runtime, as the generated fields from a logical type will be annotated with `@kotlinx.serialization.Contextual`:
     *
     * ```
     * Avro {
     *     serializersModule = SerializersModule {
     *         contextual(YourLogicalTypeClassSerializer())
     *     }
     * }
     * ```
     *
     * It can enable different serializers depending on the context (one for json, one for avro, ...).
     *
     * To specify the serializer at compile-time (recommended), use [asType] instead.
     */
    public fun asContextualType(className: String) {
        whenBuilt(logicalTypeName, LogicalTypeMapping(className, null))
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
    public abstract val logicalTypes: MapProperty<String, LogicalTypeMapping>

    @get:Input
    public abstract val useKotlinConventionForFieldNames: Property<Boolean>

    @TaskAction
    public fun generateKotlinSources() {
        val logicalTypes = logicalTypes.get()
        val fieldNamingStrategy =
            if (useKotlinConventionForFieldNames.getOrElse(false)) {
                FieldNamingStrategy.CamelCase
            } else {
                FieldNamingStrategy.Identity
            }

        val kotlinGenerator =
            KotlinGenerator(
                fieldNamingStrategy = fieldNamingStrategy,
                additionalLogicalTypes =
                    logicalTypes.map {
                        KotlinGenerator.LogicalTypeDescriptor(
                            logicalTypeName = it.key,
                            kotlinClassName = it.value.type,
                            kSerializerClassName = it.value.serializerType
                        )
                    }
            )
        val outputDir = outputDir.asFile.get()
        val files = getInputAvroSchemaFiles()

        if (files.isEmpty()) {
            val msg = "No files to generate as there are no input schema files. Please configure avro4k.inputFiles accordingly."
            logger.error(msg)
            error(msg)
        }

        kotlinGenerator.generateKotlinClassesFromFiles(files)
            .associate { (file, generatedFiles) ->
                logger.lifecycle("Schema $file generated ${generatedFiles.size} class(es):\n " + generatedFiles.joinToString("\n ") { it.fullName() })
                file to generatedFiles
            }
            .also { it.verifyNoNameClash() }
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
            .sorted()

    private fun FileSpec.fullName() = if (packageName.isEmpty()) name else "$packageName.$name"

    private fun Map<File, List<FileSpec>>.verifyNoNameClash() {
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
                "Duplicate generated class names found with different schema files. " +
                    "This usually indicates that the same full-name is used across multiple avro schema files but with different content. " +
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