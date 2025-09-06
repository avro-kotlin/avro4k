group = "com.github.avro-kotlin.avro4k"

tasks.register("actionsBeforeCommit") {
    group = "verification"
    val tasksToBeRun = listOf(
        "classes",
        "testClasses",
        "apiDump",
        "spotlessApply"
    )
    subprojects {
        tasksToBeRun.forEach {task ->
            tasks.findByName(task)?.let { dependsOn(it) }
        }
    }
}