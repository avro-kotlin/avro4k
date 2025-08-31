plugins {
    id("library-module-conventions")
    id("idea")
}

val testExpectedGeneratedSources by sourceSets.creating
val testExpectedGeneratedSourcesImplementation by configurations.getting

file("src/test/expected-sources").list().forEach { testCase ->
    val sourceSet = sourceSets.create("testExpectedSources_$testCase")
    sourceSet.kotlin.srcDir(file("src/test/expected-sources/$testCase"))

    val implementation = configurations.getByName(sourceSet.implementationConfigurationName)
    implementation.extendsFrom(testExpectedGeneratedSourcesImplementation)

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

    testExpectedGeneratedSourcesImplementation(project(":core"))
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