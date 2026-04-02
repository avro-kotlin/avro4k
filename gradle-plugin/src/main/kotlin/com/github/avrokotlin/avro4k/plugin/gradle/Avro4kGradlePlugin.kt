package com.github.avrokotlin.avro4k.plugin.gradle

import org.gradle.api.Action
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.Nested
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.register

public abstract class Avro4kPluginExtension {
    @get:Nested
    public abstract val sourcesGeneration: Avro4kPluginSourcesGenerationExtension

    public fun sourcesGeneration(action: Action<in Avro4kPluginSourcesGenerationExtension>): Unit =
        action.execute(sourcesGeneration)
}

public class Avro4kGradlePlugin : Plugin<Project> {
    override fun apply(project: Project) {
        val extension = project.extensions.create<Avro4kPluginExtension>("avro4k")
        extension.sourcesGeneration.outputDir.convention(project.layout.buildDirectory.dir("generated/sources/avro/main"))
        @Suppress("UnstableApiUsage")
        extension.sourcesGeneration.inputSchemas.convention(project.layout.projectDirectory.dir("src/main/avro"))
        extension.sourcesGeneration.useKotlinConventionForFieldNames.convention(false)

        val task =
            project.tasks.register<GenerateKotlinAvroSourcesTask>("generateAvroKotlinSources") {
                group = "build"
                description = "Generates Avro Kotlin source files from avro schemas"

                inputFiles.setFrom(extension.sourcesGeneration.inputSchemas)
                outputDir.set(extension.sourcesGeneration.outputDir)
                useKotlinConventionForFieldNames.set(extension.sourcesGeneration.useKotlinConventionForFieldNames)
                logicalTypes.set(extension.sourcesGeneration.logicalTypes.mappings)
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