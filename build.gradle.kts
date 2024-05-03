import org.jetbrains.kotlin.gradle.dsl.ExplicitApiMode
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    repositories {
        mavenCentral()
        mavenLocal()
        google()
        gradlePluginPortal()
    }
}

plugins {
    java
    id("java-library")
    kotlin("jvm") version libs.versions.kotlin
    kotlin("plugin.serialization") version libs.versions.kotlin
    id("maven-publish")
    signing
    alias(libs.plugins.dokka)
    alias(libs.plugins.kover)
    alias(libs.plugins.kotest)
    alias(libs.plugins.github.versions)
    alias(libs.plugins.nexus.publish)
    alias(libs.plugins.spotless)
    id("org.jetbrains.kotlinx.binary-compatibility-validator") version "0.14.0"
}

tasks {
    javadoc
}

group = "com.github.avro-kotlin.avro4k"
version = Ci.publishVersion

dependencies {
    api(libs.apache.avro)
    api(libs.kotlinx.serialization.core)
    implementation(libs.kotlinx.serialization.json)
    implementation(kotlin("reflect"))
    testImplementation(libs.kotest.junit5)
    testImplementation(libs.kotest.core)
    testImplementation(libs.kotest.json)
    testImplementation(libs.kotest.property)
}

kotlin {
    explicitApi = ExplicitApiMode.Strict
}

apiValidation {
    nonPublicMarkers.add("kotlinx.serialization.ExperimentalSerializationApi")
}

tasks.withType<KotlinCompile>().configureEach {
    kotlinOptions.jvmTarget = "1.8"
    kotlinOptions.apiVersion = "1.6"
    kotlinOptions.languageVersion = "1.6"
    kotlinOptions.freeCompilerArgs +=
        listOf(
            "-Xexplicit-api=strict",
            "-opt-in=kotlinx.serialization.ExperimentalSerializationApi",
            "-opt-in=kotlin.RequiresOptIn",
            "-Xcontext-receivers"
        )
}
java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withJavadocJar()
    withSourcesJar()
}
tasks.named<Test>("test") {
    useJUnitPlatform()
    filter {
        isFailOnNoMatchingTests = false
    }
    testLogging {
        showExceptions = true
        showStandardStreams = true
        events =
            setOf(
                org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED,
                org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED
            )
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
tasks.named<Jar>("javadocJar") {
    from(tasks.named("dokkaJavadoc"))
}

// Configuration for publication on maven central
val signingKey: String? by project
val signingPassword: String? by project

val publications: PublicationContainer = (extensions.getByName("publishing") as PublishingExtension).publications

signing {
    useGpgCmd()
    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
    }
    if (Ci.isRelease) {
        sign(publications)
    }
}

nexusPublishing {
    this.repositories {
        sonatype()
    }
}
publishing {
    publications {
        register("mavenJava", MavenPublication::class) {
            from(components["java"])
            pom {
                val projectUrl = "https://github.com/avro-kotlin/avro4k"
                name.set("avro4k-core")
                description.set("Avro format support for kotlinx.serialization")
                url.set(projectUrl)

                scm {
                    connection.set("scm:git:$projectUrl")
                    developerConnection.set("scm:git:$projectUrl")
                    url.set(projectUrl)
                }

                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://opensource.org/licenses/Apache-2.0")
                    }
                }

                developers {
                    developer {
                        id.set("thake")
                        name.set("Thorsten Hake")
                        email.set("mail@thorsten-hake.com")
                    }
                    developer {
                        id.set("chuckame")
                        name.set("Antoine Michaud")
                        email.set("contact@antoine-michaud.fr")
                    }
                }
            }
        }
    }
}

fun Project.publishing(action: PublishingExtension.() -> Unit) = configure(action)

fun Project.signing(configure: SigningExtension.() -> Unit): Unit = configure(configure)

object Ci {
    // this is the version used for building snapshots
    // .buildnumber-snapshot will be appended
    private const val SNAPSHOT_BASE = "1.9.0"

    private val githubBuildNumber = System.getenv("GITHUB_RUN_NUMBER")

    private val snapshotVersion =
        when (githubBuildNumber) {
            null -> "$SNAPSHOT_BASE-SNAPSHOT"
            else -> "$SNAPSHOT_BASE.$githubBuildNumber-SNAPSHOT"
        }

    private val releaseVersion = System.getenv("RELEASE_VERSION")

    val isRelease = releaseVersion != null
    val publishVersion = releaseVersion ?: snapshotVersion
}

spotless {
    kotlin {
        ktlint()
    }
    json {
        target("src/test/resources/**.json")
        prettier()
    }
    kotlinGradle {
        ktlint()
    }
}