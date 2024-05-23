pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    id("com.gradle.enterprise") version ("3.16.2")
}

rootProject.name = "avro4k-core"

include("benchmark")

gradleEnterprise {
    if (System.getenv("CI") != null) {
        buildScan {
            publishAlways()
            termsOfServiceUrl = "https://gradle.com/terms-of-service"
            termsOfServiceAgree = "yes"
        }
    }
}

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            version("kotlin", "2.0.0")
            version("jvm", "21")

            library("apache-avro", "org.apache.avro", "avro").version("1.11.3")

            val kotlinxSerialization = "1.7.0-RC"
            library("kotlinx-serialization-core", "org.jetbrains.kotlinx", "kotlinx-serialization-core").version(kotlinxSerialization)
            library("kotlinx-serialization-json", "org.jetbrains.kotlinx", "kotlinx-serialization-json").version(kotlinxSerialization)

            val kotestVersion = "5.8.0"
            library("kotest-core", "io.kotest", "kotest-assertions-core").version(kotestVersion)
            library("kotest-json", "io.kotest", "kotest-assertions-json").version(kotestVersion)
            library("kotest-junit5", "io.kotest", "kotest-runner-junit5").version(kotestVersion)
            library("kotest-property", "io.kotest", "kotest-property").version(kotestVersion)

            plugin("dokka", "org.jetbrains.dokka").version("1.9.10")
            plugin("kotest", "io.kotest").version("0.4.11")
            plugin("github-versions", "com.github.ben-manes.versions").version("0.46.0")
            plugin("nexus-publish", "io.github.gradle-nexus.publish-plugin").version("1.3.0")
            plugin("spotless", "com.diffplug.spotless").version("6.25.0")
            plugin("kover", "org.jetbrains.kotlinx.kover").version("0.7.6")
        }
    }
    @Suppress("UnstableApiUsage")
    repositories {
        mavenCentral()
    }
}