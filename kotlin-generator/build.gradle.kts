plugins {
    id("library-module-conventions")
    id("library-publish-conventions")
    id("idea")
}

description = "Avro4k's compatible code generator"

val commonExpectedGeneratedSources by sourceSets.creating {
    kotlin.srcDir(file("src/test/expected-sources-common"))
}
tasks.named("testClasses") {
    dependsOn(tasks.named(commonExpectedGeneratedSources.classesTaskName))
}
idea.module.testSources += commonExpectedGeneratedSources.kotlin
val commonExpectedGeneratedSourcesImplementation by configurations.getting

file("src/test/expected-sources").list().forEach { testCase ->
    val sourceSet = sourceSets.create("testExpectedSources_$testCase")
    sourceSet.kotlin.srcDir(file("src/test/expected-sources/$testCase"))

    // Make the test-case's sourceSet using the common
    val implementation = configurations.getByName(sourceSet.implementationConfigurationName)
    implementation.extendsFrom(commonExpectedGeneratedSourcesImplementation)

    tasks.named(sourceSet.classesTaskName) {
        dependsOn(tasks.named(commonExpectedGeneratedSources.classesTaskName))
    }
    // make them green in IntelliJ !
    idea.module.testSources += sourceSet.kotlin
}

dependencies {
    api(libs.kotlinpoet) {
        exclude(module = "kotlin-reflect")
    }
    implementation(project(":core"))

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