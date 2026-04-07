plugins {
    kotlin("multiplatform")
    id("org.jetbrains.dokka")
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
        freeCompilerArgs = listOf(
            "-Xcontext-parameters",
            "-Xexpect-actual-classes",
        )
    }

    jvmToolchain(11)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

spotless {
    val ktlintVersion = extensions.getByType<VersionCatalogsExtension>().named("libs").findVersion("ktlint").get().toString()
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