plugins {
    java
    kotlin("jvm") version libs.versions.kotlin
    kotlin("plugin.serialization") version libs.versions.kotlin
}

dependencies {
    implementation(project(":"))
    implementation(kotlin("reflect"))

    api(libs.confluent.kafka.avro.serializer) {
        // avro4k declares its own avro dependency
        exclude(group = "org.apache.avro")
        exclude(group = "org.apache.commons", module = "commons-compress")
    }
    testImplementation(libs.kotest.junit5)
    testImplementation(libs.kotest.core)
    testImplementation(libs.mockk)
    testImplementation(kotlin("reflect"))
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
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_1_8)
        apiVersion.set(org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_1_9)
        languageVersion.set(org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_1_9)
        freeCompilerArgs = listOf("-Xcontext-receivers")
    }
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}
repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
}