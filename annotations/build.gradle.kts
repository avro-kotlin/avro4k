plugins {
    id("library-module-conventions")
    id("library-publish-conventions")
}

description = "Contains only the annotations of avro4k to allow other components to depend on them without pulling in the whole avro4k library."

dependencies {
    // Only used for @SerialInfo on annotations
    compileOnly(libs.kotlinx.serialization.core)
}