plugins {
    java
    id("java-library")
    kotlin("jvm") version libs.versions.kotlin
    kotlin("plugin.serialization") version libs.versions.kotlin
    id("maven-publish")
    signing
    alias(libs.plugins.dokka)
    alias(libs.plugins.dokka.javadoc)
    alias(libs.plugins.kover)
    alias(libs.plugins.github.versions)
    alias(libs.plugins.nexus.publish)
    alias(libs.plugins.spotless)
    alias(libs.plugins.binary.compatibility.validator)
}

group = "com.github.avro-kotlin.avro4k"
version = Ci.publishVersion

dependencies {
    api(libs.apache.avro)
    api(libs.kotlinx.serialization.core)
    api(libs.kotlinx.io)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.okio)
    testImplementation(libs.kotest.junit5)
    testImplementation(libs.kotest.core)
    testImplementation(libs.kotest.json)
    testImplementation(libs.kotest.property)
    testImplementation(libs.mockk)
    testImplementation(kotlin("reflect"))
}

kotlin {
    explicitApi()

    compilerOptions {
        optIn = listOf("kotlin.RequiresOptIn", "kotlinx.serialization.ExperimentalSerializationApi")
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_1_8)
        apiVersion.set(org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_1_9)
        languageVersion.set(org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_1_9)
        freeCompilerArgs = listOf("-Xcontext-receivers")
    }
}
java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withSourcesJar()
}
tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

val dokkaJavadocJar by tasks.register<Jar>("dokkaJavadocJar") {
    dependsOn(tasks.dokkaGeneratePublicationJavadoc)
    from(tasks.dokkaGeneratePublicationJavadoc.flatMap { it.outputDirectory })
    archiveClassifier.set("javadoc")
}

val dokkaHtmlJar by tasks.register<Jar>("dokkaHtmlJar") {
    dependsOn(tasks.dokkaGeneratePublicationHtml)
    from(tasks.dokkaGeneratePublicationHtml.flatMap { it.outputDirectory })
    archiveClassifier.set("html-docs")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifact(dokkaJavadocJar)
            artifact(dokkaHtmlJar)
            pom {
                val projectUrl = "https://github.com/avro-kotlin/avro4k"
                name.set("avro4k-core")
                description.set("Avro binary format support for kotlin, built on top of kotlinx-serialization")
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

nexusPublishing {
    repositories {
        sonatype()
    }
}

signing {
    if (Ci.isRelease) {
        val signingKey: String? by project
        val signingPassword: String? by project

        if (signingKey != null && signingPassword != null) {
            useInMemoryPgpKeys(signingKey, signingPassword)
        } else {
            throw IllegalStateException("No signing key or password found")
        }
        sign(publishing.publications)
    }
}

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

task("actionsBeforeCommit") {
    this.group = "verification"
    dependsOn("apiDump")
    dependsOn("spotlessApply")
    dependsOn("test")
    dependsOn("koverLog")
}

repositories {
    mavenCentral()
}