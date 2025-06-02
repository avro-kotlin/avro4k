plugins {
    java
    kotlin("jvm") version libs.versions.kotlin
    kotlin("plugin.serialization") version libs.versions.kotlin
}

dependencies {
    implementation(project(":"))

    // TODO remove it
    api("io.confluent:kafka-schema-serializer:7.8.0")

    api("io.confluent:kafka-schema-registry-client:7.8.0") {
        // we don't want the kafka ccs version of kafka-clients
        exclude(group = "org.apache.kafka", module = "kafka-clients")
        exclude(group = "org.apache.avro")
        exclude(group = "org.yaml")
        exclude(group = "org.apache.commons", module = "commons-compress")
    }
    implementation(libs.okio)
    implementation("org.apache.kafka:kafka-clients:3.9.0")

    testImplementation("io.confluent:kafka-avro-serializer:7.8.0")
    testImplementation(libs.kotest.junit5)
    testImplementation(libs.kotest.core)
    testImplementation(libs.mockk)
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.12.0")
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
        optIn = listOf("kotlin.RequiresOptIn", "kotlinx.serialization.ExperimentalSerializationApi", "com.github.avrokotlin.avro4k.InternalAvro4kApi", "com.github.avrokotlin.avro4k.ExperimentalAvro4kApi")
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
}