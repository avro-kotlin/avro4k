pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

rootProject.name = "avro4k-core"

include("benchmark")
include("confluent-kafka-serializer")