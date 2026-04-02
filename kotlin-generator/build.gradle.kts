plugins {
    id("library-module-conventions")
    id("library-publish-conventions")
    id("idea")
    kotlin("plugin.serialization")
}

description = "Avro4k's compatible code generator"

val commonExpectedGeneratedSources by sourceSets.creating {
    kotlin.srcDir(file("src/test/expected-sources-common"))
}
idea.module.testSources += commonExpectedGeneratedSources.kotlin
val commonExpectedGeneratedSourcesImplementation by configurations.getting

val allExpectedSourceSets =
    file("src/test/expected-sources").list().map { testCase ->
        val sourceSet = sourceSets.create("testExpectedSources_$testCase")
        sourceSet.kotlin.srcDir(file("src/test/expected-sources/$testCase"))

        // Make the test-case's sourceSet using the common
        val implementation = configurations.getByName(sourceSet.implementationConfigurationName)
        implementation.extendsFrom(commonExpectedGeneratedSourcesImplementation)

        // Add this line - make the sourceSet depend on common source set output
        sourceSet.compileClasspath += commonExpectedGeneratedSources.output
        sourceSet.runtimeClasspath += commonExpectedGeneratedSources.output

        tasks.named(sourceSet.classesTaskName) {
            dependsOn(tasks.named(commonExpectedGeneratedSources.classesTaskName))
        }
        // make them green in IntelliJ !
        idea.module.testSources += sourceSet.kotlin
        sourceSet
    }
val testExpectedSourcesClasses by tasks.registering {
    group = "build"
    description = "Compile all expected-sources source sets at once"

    allExpectedSourceSets.forEach { sourceSet ->
        dependsOn(tasks.named(sourceSet.classesTaskName))
    }
}
tasks.named("testClasses") {
    dependsOn(tasks.named(commonExpectedGeneratedSources.classesTaskName))
    dependsOn(testExpectedSourcesClasses)
}

dependencies {
    api(libs.kotlinpoet) {
        exclude(module = "kotlin-reflect")
    }
    implementation(project(":core"))
    implementation(libs.kotlinx.serialization.json)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.kotest.core)
    testImplementation(libs.kotest.junit5)
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    commonExpectedGeneratedSourcesImplementation(project(":core"))
}

spotless {
    kotlin {
        targetExclude("src/test/expected-sources/**")
    }
    json {
        target("src/test/resources/**.avsc")
        gson()
    }
}