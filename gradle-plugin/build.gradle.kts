import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    `java-gradle-plugin`
    id("library-module-conventions")
    id("org.jetbrains.kotlin.plugin.sam.with.receiver")
    id("com.github.gmazzo.buildconfig")
    id("com.gradle.plugin-publish")
}

description = "Avro4k's gradle plugin, enabling kotlin code generation from avro schemas to ensure type safety in your kafka/avro apps."

// Not using `kotlin-dsl` as it is tied to the Kotlin version embedded in Gradle, which conflicts with the project's Kotlin version.
// Instead, we replicate what it does: Kotlin DSL API, implicit receiver for gradle's Action lambdas,
// and targeting the Kotlin version embedded in the oldest supported Gradle version (9.0) to keep the plugin loadable.
samWithReceiver {
    annotation("org.gradle.api.HasImplicitReceiver")
}

kotlin {
    compilerOptions {
        apiVersion = KotlinVersion.KOTLIN_2_2
        languageVersion = KotlinVersion.KOTLIN_2_2
        // avro4k's opt-in markers are not visible from this module
        optIn.empty()
    }
}

dependencies {
    compileOnly(gradleKotlinDsl())
    implementation(kotlin("gradle-plugin"))
    implementation(project(":kotlin-generator"))

    testImplementation(gradleKotlinDsl())
    testImplementation(gradleTestKit())
    testImplementation(kotlin("test"))
    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.kotest.core)
}

tasks.test {
    useJUnitPlatform()
    // The plugin also injects project's bom & core modules, so we need to publish them first to mavenLocal
    dependsOn(":bom:publishToMavenLocal")
    dependsOn(":core:publishToMavenLocal")
}

buildConfig {
    buildConfigField("AVRO4K_VERSION", provider { rootProject.version.toString() })
    packageName = "com.github.avrokotlin.avro4k.plugin.gradle"
}

spotless.kotlin {
    // exclude buildConfig generated files
    targetExclude("build/generated/**")
}

gradlePlugin {
    website = "https://github.com/avro-kotlin/avro4k"
    vcsUrl = "https://github.com/avro-kotlin/avro4k.git"
    plugins {
        create(rootProject.name) {
            id = "io.github.avro-kotlin"
            group = "io.github.avro-kotlin"
            displayName = "Avro4k Gradle Plugin"
            description = project.description
            version = project.version
            implementationClass = "com.github.avrokotlin.avro4k.plugin.gradle.Avro4kGradlePlugin"
            tags = listOf("kotlin", "avro", "serialization", "kafka", "codegen")
        }
    }
}