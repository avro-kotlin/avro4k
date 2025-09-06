plugins {
    id("library-module-conventions")
    id("idea")
}

val commonExpectedGeneratedSources by sourceSets.creating
val commonExpectedGeneratedSourcesImplementation by configurations.getting

file("src/test/expected-sources").list().forEach { testCase ->
    val sourceSet = sourceSets.create("testExpectedSources_$testCase")
    sourceSet.kotlin.srcDir(file("src/test/expected-sources/$testCase"))

    // Make the test-case's sourceSet using the common
    val implementation = configurations.getByName(sourceSet.implementationConfigurationName)
    implementation.extendsFrom(commonExpectedGeneratedSourcesImplementation)

    tasks.named("testClasses") {
        dependsOn(tasks.named(sourceSet.classesTaskName))
    }
    // make them green in IntelliJ !
    idea.module.testSources += sourceSet.kotlin
}

dependencies {
    implementation(libs.kotlinpoet) {
        exclude(module = "kotlin-reflect")
    }
    api(project(":core"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.kotest.core)
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    commonExpectedGeneratedSourcesImplementation(project(":core"))
}

repositories {
    mavenCentral()
}

spotless {
    kotlin {
        targetExclude("src/test/expected-sources/**")
    }
    json {
        target("src/test/resources/**.avsc")
        prettier().config(
            mapOf(
                "tabWidth" to 4
            )
        )
    }
}