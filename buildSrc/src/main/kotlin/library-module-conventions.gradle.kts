import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

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
    withSourcesJar()
}

kotlin {
    explicitApi()

    compilerOptions {
        optIn = listOf(
            "com.github.avrokotlin.avro4k.InternalAvro4kApi",
            "com.github.avrokotlin.avro4k.ExperimentalAvro4kApi",
        )
        jvmToolchain(11)
    }
}

// JUnit 6 and Gradle 9 (gradle-plugin tests) require Java 17+, so tests are compiled and run on a newer JVM
// while the published code still targets Java 11
val testToolchain = Action<JavaToolchainSpec> { languageVersion = JavaLanguageVersion.of(21) }

// Gradle derives the JVM version requested when resolving the test classpaths from this task, even without java sources
tasks.named<JavaCompile>("compileTestJava") {
    javaCompiler = javaToolchains.compilerFor(testToolchain)
}

tasks.named<KotlinCompile>("compileTestKotlin") {
    kotlinJavaToolchain.toolchain.use(javaToolchains.launcherFor(testToolchain))
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    javaLauncher = javaToolchains.launcherFor(testToolchain)
}

spotless {
    val ktlintVersion = versionCatalogs.named("libs").findVersion("ktlint").get().toString()
    kotlin {
        ktlint(ktlintVersion).setEditorConfigPath(rootProject.file(".editorconfig"))
    }
    kotlinGradle {
        ktlint(ktlintVersion).setEditorConfigPath(rootProject.file(".editorconfig"))
    }
}

repositories {
    mavenCentral()
}