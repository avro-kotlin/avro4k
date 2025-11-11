tasks.register("actionsBeforeCommit") {
    group = "verification"
    val tasksToBeRun = listOf(
        "classes",
        "testClasses",
        "apiDump",
        "spotlessApply"
    )
    subprojects {
        tasksToBeRun.forEach { task ->
            tasks.findByName(task)?.let { dependsOn(it) }
        }
    }
}

repositories {
    // Need maven central here and not into a buildSrc module to allow IntelliJ downloading sources
    mavenCentral()
}
