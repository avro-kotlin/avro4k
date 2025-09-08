plugins {
    id("library-module-conventions")
    kotlin("plugin.serialization")
}

dependencies {
    api(libs.apache.avro)
    api(libs.kotlinx.serialization.core)
    api(libs.kotlinx.io)
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