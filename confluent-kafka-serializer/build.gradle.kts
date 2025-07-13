plugins {
    java
    kotlin("jvm") version libs.versions.kotlin
    kotlin("plugin.serialization") version libs.versions.kotlin
}

dependencies {
    implementation(project(":"))

    // TODO remove it
    api("io.confluent:kafka-schema-serializer:7.8.0") {
        // avro4k declares its own avro dependency
        exclude(group = "org.apache.avro")
    }

    api("io.confluent:kafka-schema-registry-client:7.8.0") {
        // avro4k declares its own avro dependency
        exclude(group = "org.apache.avro")
    }

    testImplementation("io.confluent:kafka-avro-serializer:7.8.0") {
        // avro4k declares its own avro dependency
        exclude(group = "org.apache.avro")
    }
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
        optIn = listOf(
//            "kotlin.RequiresOptIn",
//            "kotlinx.serialization.ExperimentalSerializationApi",
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
}