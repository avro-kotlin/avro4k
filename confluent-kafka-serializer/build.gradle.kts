plugins {
    id("library-module-conventions")
    id("library-publish-conventions")
    kotlin("plugin.serialization")
}

description = "Avro4k's module to enable (de)serializing kotlin classes and other native types in avro, with a schema registry."

dependencies {
    implementation(project(":core"))
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

repositories {
    maven("https://packages.confluent.io/maven")
}