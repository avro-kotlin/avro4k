plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
}

dependencies {
    val kotlinVersion = "2.2.21"

    implementation("org.jetbrains.kotlin.jvm:org.jetbrains.kotlin.jvm.gradle.plugin:$kotlinVersion")
    implementation("org.jetbrains.kotlin.plugin.allopen:org.jetbrains.kotlin.plugin.allopen.gradle.plugin:$kotlinVersion")
    implementation("org.jetbrains.kotlin.plugin.noarg:org.jetbrains.kotlin.plugin.noarg.gradle.plugin:$kotlinVersion")
    implementation("org.jetbrains.kotlin.plugin.serialization:org.jetbrains.kotlin.plugin.serialization.gradle.plugin:$kotlinVersion")

    implementation("org.jetbrains.kotlinx.binary-compatibility-validator:org.jetbrains.kotlinx.binary-compatibility-validator.gradle.plugin:0.18.1")

    implementation("com.github.gmazzo.buildconfig:com.github.gmazzo.buildconfig.gradle.plugin:6.0.7")
    implementation("com.gradle.plugin-publish:com.gradle.plugin-publish.gradle.plugin:2.0.0")

    implementation("com.diffplug.spotless:com.diffplug.spotless.gradle.plugin:7.2.1")

    implementation("com.vanniktech.maven.publish:com.vanniktech.maven.publish.gradle.plugin:0.35.0")

    val dokkaVersion = "2.1.0"
    implementation("org.jetbrains.dokka:org.jetbrains.dokka.gradle.plugin:$dokkaVersion")
    implementation("org.jetbrains.dokka-javadoc:org.jetbrains.dokka-javadoc.gradle.plugin:$dokkaVersion")
}
