group = "com.github.avro-kotlin.avro4k"
version = System.getenv("RELEASE_VERSION") ?: "local-SNAPSHOT"

tasks.register("actionsBeforeCommit") {
    this.group = "verification"
    dependsOn(":core:apiDump")
    dependsOn(":core:spotlessApply")
    dependsOn(":confluent-kafka-serializer:apiDump")
    dependsOn(":confluent-kafka-serializer:spotlessApply")
}