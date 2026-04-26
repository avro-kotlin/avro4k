plugins {
    id("library-module-conventions")
    id("library-publish-conventions")
    kotlin("plugin.serialization")
}

description = "Core module of avro4k. Avro4k is the avro binary format support for kotlin, built on top of kotlinx-serialization."

dependencies {
    api(libs.apache.avro)
    api(libs.kotlinx.serialization.core)
    api(libs.kotlinx.io)
    api(libs.kotlinx.datetime)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.okio)

    testImplementation(libs.kotest.junit5)
    testImplementation(libs.kotest.core)
    testImplementation(libs.mockk)
    testImplementation(kotlin("reflect"))
}

spotless {
    json {
        target("**.json")
        prettier()
    }
}