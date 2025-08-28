plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
}

dependencies {
    implementation("org.jetbrains.kotlin.jvm:org.jetbrains.kotlin.jvm.gradle.plugin:2.2.10")
    implementation("org.jetbrains.kotlin.plugin.allopen:org.jetbrains.kotlin.plugin.allopen.gradle.plugin:2.2.10")
    implementation("org.jetbrains.kotlin.plugin.noarg:org.jetbrains.kotlin.plugin.noarg.gradle.plugin:2.2.10")
    implementation("org.jetbrains.kotlin.plugin.serialization:org.jetbrains.kotlin.plugin.serialization.gradle.plugin:2.2.10")

    implementation("org.jetbrains.kotlinx.binary-compatibility-validator:org.jetbrains.kotlinx.binary-compatibility-validator.gradle.plugin:0.18.1")

    implementation("com.diffplug.spotless:com.diffplug.spotless.gradle.plugin:7.2.1")
    implementation("io.github.gradle-nexus.publish-plugin:io.github.gradle-nexus.publish-plugin.gradle.plugin:2.0.0")

    implementation("org.jetbrains.dokka:org.jetbrains.dokka.gradle.plugin:2.0.0")
    implementation("org.jetbrains.dokka-javadoc:org.jetbrains.dokka-javadoc.gradle.plugin:2.0.0")
}
