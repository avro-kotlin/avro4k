plugins {
    id("java-platform")
    id("library-publish-conventions")
}

description = "BOM for avro4k related modules and dependencies, ensuring compatible versions"

dependencies {
    constraints {
        api(libs.kotlinx.serialization.core) {
            version {
                // Versions < 1.7.0 are missing a new method: SerialDescriptorsKt.getNonNullOriginal
                reject("(,1.7.0[")
            }
        }
        api(project(":core"))
        api(project(":kotlin-generator"))
        api(project(":confluent-kafka-serializer"))
    }
}
