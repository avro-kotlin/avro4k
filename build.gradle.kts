plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.allopen) apply false
    alias(libs.plugins.kotlin.noarg) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.kotlinx.binary.compatibility.validator) apply false
    alias(libs.plugins.buildconfig) apply false
    alias(libs.plugins.gradle.plugin.publish) apply false
    alias(libs.plugins.spotless) apply false
    alias(libs.plugins.maven.publish) apply false
    alias(libs.plugins.dokka) apply false
    alias(libs.plugins.dokka.javadoc) apply false
}

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
