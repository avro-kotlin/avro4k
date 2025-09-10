plugins {
    id("java-library")
    kotlin("jvm")
    id("org.jetbrains.dokka")
    id("org.jetbrains.dokka-javadoc")
    id("org.jetbrains.kotlinx.binary-compatibility-validator")
    id("com.diffplug.spotless")
}

apiValidation {
    nonPublicMarkers += "com.github.avrokotlin.avro4k.InternalAvro4kApi"
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withSourcesJar()
}

kotlin {
    explicitApi()

    compilerOptions {
        optIn = listOf(
            "com.github.avrokotlin.avro4k.InternalAvro4kApi",
            "com.github.avrokotlin.avro4k.ExperimentalAvro4kApi",
        )
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_1_8
        apiVersion = org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_1_9
        languageVersion = org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_1_9
        freeCompilerArgs = listOf("-Xcontext-receivers")
    }
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

spotless {
    kotlin {
        ktlint()
    }
    kotlinGradle {
        ktlint()
    }
}

repositories {
    mavenCentral()
}