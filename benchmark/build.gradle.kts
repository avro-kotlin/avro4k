import kotlinx.benchmark.gradle.JvmBenchmarkTarget

plugins {
    java
    kotlin("jvm")
    id("org.jetbrains.kotlinx.benchmark") version "0.4.15"
    kotlin("plugin.allopen")
    kotlin("plugin.serialization")
    kotlin("plugin.noarg")
}

allOpen {
    annotation("org.openjdk.jmh.annotations.State")
}

noArg {
    annotation("kotlinx.serialization.Serializable")
}

benchmark {
    configurations {
        named("main") {
            reportFormat = "text"
        }
        register("simple-read") {
            include("^com.github.avrokotlin.benchmark.simple.+.read$")
        }
        register("simple-write") {
            include("^com.github.avrokotlin.benchmark.simple.+.write$")
        }
        register("complex-read") {
            include("^com.github.avrokotlin.benchmark.complex.+.read$")
        }
        register("complex-write") {
            include("^com.github.avrokotlin.benchmark.complex.+.write$")
        }
        register("avro4k") {
            include("Avro4k")
        }
        register("avro4k-read") {
            include("Avro4k.+read")
        }
        register("avro4k-write") {
            include("Avro4k.+write")
        }
        register("lists-read") {
            include("^com.github.avrokotlin.benchmark.lists.+.read$")
        }
        register("lists-write") {
            include("^com.github.avrokotlin.benchmark.lists.+.write$")
        }
    }
    targets {
        register("main")
    }
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-benchmark-runtime:0.4.15")

    val jacksonVersion = "2.20.1"
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-avro:$jacksonVersion")
    implementation("org.slf4j:slf4j-simple:2.0.17")

    implementation(project(":core"))
}

kotlin {
    compilerOptions {
        optIn = listOf(
            "com.github.avrokotlin.avro4k.ExperimentalAvro4kApi",
        )
    }
}
repositories {
    mavenCentral()
}
