plugins {
    `java-gradle-plugin`
    `kotlin-dsl`
    id("library-module-conventions")
    id("com.github.gmazzo.buildconfig")
    id("com.gradle.plugin-publish")
}

description = "Avro4k's gradle plugin, enabling kotlin code generation from avro schemas to ensure type safety in your kafka/avro apps."

dependencies {
    implementation(kotlin("gradle-plugin"))
    implementation(project(":kotlin-generator"))

    testImplementation(gradleTestKit())
    testImplementation(kotlin("test"))
    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.kotest.core)
}

kotlin {
    explicitApi()
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