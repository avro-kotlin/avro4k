plugins {
    id("org.gradle.toolchains.foojay-resolver-convention").version("1.0.0")
}

rootProject.name = "avro4k"

include("core")
include("confluent-kafka-serializer")
include("benchmark")
include("kotlin-generator")
include("bom")
include("gradle-plugin")
