plugins {
    id("library-module-conventions")
    id("library-publish-conventions")
}

description = "Contains only the annotations of avro4k to allow other components to depend on them without pulling in the whole avro4k library."

dependencies {
    implementation(libs.kotlinx.serialization.core)
}