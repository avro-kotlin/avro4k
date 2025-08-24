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
System.getenv("RELEASE_VERSION")?.let {
    version = it
}

dependencies {
    api(libs.apache.avro)
    api(libs.kotlinx.serialization.core)
    api(libs.kotlinx.io)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.okio)
    testImplementation(libs.kotest.junit5)
    testImplementation(libs.kotest.core)
    testImplementation(libs.mockk)
    testImplementation(kotlin("reflect"))
}

apiValidation {
    nonPublicMarkers += "com.github.avrokotlin.avro4k.InternalAvro4kApi"
}

kotlin {
    explicitApi()

    compilerOptions {
        optIn = listOf("kotlin.RequiresOptIn", "kotlinx.serialization.ExperimentalSerializationApi", "com.github.avrokotlin.avro4k.InternalAvro4kApi")
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
    setRequired {
        // signing is required if this is a release version and the artifacts are to be published
        // do not use hasTask() as this require realization of the tasks that maybe are not necessary
        (System.getenv("RELEASE_VERSION")?.let { !it.endsWith("-SNAPSHOT") } ?: false) &&
            gradle.taskGraph.allTasks.any { it is PublishToMavenRepository }
    }
    sign(publishing.publications["mavenJava"])
}

spotless {
    kotlin {
        ktlint()
        // test
    }
    json {
        target("src/test/resources/**.json")
        prettier()
    }
    kotlinGradle {
        ktlint()
    }
}

tasks.register("commitPrePush") {
    this.group = "verification"
    dependsOn("apiDump")
    dependsOn("spotlessApply")
    // titi
}

copy {
    println("Copying git hooks")
    // noooooo
    from("$rootDir/.githooks/pre-push")
    into("$rootDir/.git/hooks")
    // coucou 2 zd erdgdtgd sr gdtg  testeur
    filePermissions {
        user { execute = true }
    }
}

repositories {
    mavenCentral()
}