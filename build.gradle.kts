plugins {
    id("io.github.gradle-nexus.publish-plugin")
}

group = "com.github.avro-kotlin.avro4k"
version = System.getenv("RELEASE_VERSION") ?: "local-SNAPSHOT"

nexusPublishing {
    repositories {
        sonatype()
    }
}

tasks.register("actionsBeforeCommit") {
    this.group = "verification"
    dependsOn(":core:apiDump")
    dependsOn(":core:spotlessApply")
    dependsOn(":confluent-kafka-serializer:apiDump")
    dependsOn(":confluent-kafka-serializer:spotlessApply")
}

repositories {
    mavenCentral()
}