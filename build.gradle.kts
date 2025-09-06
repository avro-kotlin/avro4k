group = "com.github.avro-kotlin.avro4k"

tasks.register("actionsBeforeCommit") {
    this.group = "verification"
    val tasksToBeRun = listOf(
        "apiDump",
        "spotlessApply"
    )
    subprojects {
        tasksToBeRun.forEach {task ->
            tasks.findByName(task)?.let { dependsOn(it) }
        }
    }
}