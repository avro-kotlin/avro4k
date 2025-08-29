plugins {
    id("java-library")
    kotlin("jvm")
    id("maven-publish")
    signing
    id("org.jetbrains.dokka")
    id("org.jetbrains.dokka-javadoc")
    id("org.jetbrains.kotlinx.binary-compatibility-validator")
    id("com.diffplug.spotless")
}

group = rootProject.group
version = rootProject.version

apiValidation {
    nonPublicMarkers += "com.github.avrokotlin.avro4k.InternalAvro4kApi"
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withSourcesJar()
}
kotlin {
    explicitApi()

    compilerOptions {
        optIn = listOf(
            "com.github.avrokotlin.avro4k.InternalAvro4kApi",
            "com.github.avrokotlin.avro4k.ExperimentalAvro4kApi",
        )
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_1_8
        apiVersion = org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_1_9
        languageVersion = org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_1_9
        freeCompilerArgs = listOf("-Xcontext-receivers")
    }
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

val dokkaJavadocJar by tasks.register<Jar>("dokkaJavadocJar") {
    dependsOn(tasks.dokkaGeneratePublicationJavadoc)
    from(tasks.dokkaGeneratePublicationJavadoc.flatMap { it.outputDirectory })
    archiveClassifier = "javadoc"
}

val dokkaHtmlJar by tasks.register<Jar>("dokkaHtmlJar") {
    dependsOn(tasks.dokkaGeneratePublicationHtml)
    from(tasks.dokkaGeneratePublicationHtml.flatMap { it.outputDirectory })
    archiveClassifier = "html-docs"
}

publishing {
    publications {
        create<MavenPublication>(project.name) {
            from(components["java"])
            artifact(dokkaJavadocJar)
            artifact(dokkaHtmlJar)
            pom {
                val projectUrl = "https://github.com/avro-kotlin/avro4k"
                name = project.name
                artifactId = "avro4k-${project.name}"
                description = "Avro binary format support for kotlin, built on top of kotlinx-serialization."
                url = projectUrl

                scm {
                    connection = "scm:git:$projectUrl"
                    developerConnection = "scm:git:$projectUrl"
                    url = projectUrl
                }

                licenses {
                    license {
                        name = "Apache-2.0"
                        url = "https://opensource.org/licenses/Apache-2.0"
                    }
                }

                developers {
                    developer {
                        id = "thake"
                        name = "Thorsten Hake"
                        email = "mail@thorsten-hake.com"
                    }
                    developer {
                        id = "chuckame"
                        name = "Antoine Michaud"
                        email = "contact@antoine-michaud.fr"
                    }
                }
            }
        }
    }
}

signing {
    if (System.getenv("RELEASE_VERSION") != null) {
        val signingKey = System.getenv("SIGNING_KEY")
        val signingPassword = System.getenv("SIGNING_PASSWORD")

        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications)
    }
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

repositories {
    mavenCentral()
}