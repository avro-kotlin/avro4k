buildscript {
   repositories {
      mavenCentral()
      mavenLocal()
      google()
      maven("https://dl.bintray.com/kotlin/kotlin-eap")
      maven("https://kotlin.bintray.com/kotlinx")
      gradlePluginPortal()
   }
}

plugins {
   java
   id("java-library")
   kotlin("jvm") version Libs.kotlinVersion
   id("org.jetbrains.kotlin.plugin.serialization") version "1.4.0"
   id("maven-publish")
   signing
   id("org.jetbrains.dokka") version Libs.dokkaVersion
   id("io.kotest") version Libs.kotestGradlePlugin
}

repositories {
   mavenCentral()
   jcenter()
   google()
   maven("https://dl.bintray.com/kotlin/kotlin-eap")
   maven("https://kotlin.bintray.com/kotlinx")
}

tasks {
   javadoc {
   }
}

group = "com.sksamuel.avro4k"
version = Ci.publishVersion

dependencies {
   implementation(kotlin("reflect"))
   api(Libs.Avro.avro)
   api(Libs.Kotlinx.serialization)
   implementation(Libs.Xerial.snappy)
   testImplementation(Libs.Kotest.junit5)
   testImplementation(Libs.Kotest.assertionsCore)
   testImplementation(Libs.Kotest.assertionsJson)
   testImplementation(Libs.Kotest.proptest)
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
   kotlinOptions.jvmTarget = "1.8"
   kotlinOptions.apiVersion = "1.3"
}

val ossrhUsername: String by project
val ossrhPassword: String by project
val signingKey: String? by project
val signingPassword: String? by project

fun Project.publishing(action: PublishingExtension.() -> Unit) =
   configure(action)

fun Project.signing(configure: SigningExtension.() -> Unit): Unit =
   configure(configure)

//val dokka = tasks.named("dokka")
val javadoc = tasks.named("javadoc")

val publications: PublicationContainer = (extensions.getByName("publishing") as PublishingExtension).publications

signing {
   useGpgCmd()
   if (signingKey != null && signingPassword != null) {
      @Suppress("UnstableApiUsage")
      useInMemoryPgpKeys(signingKey, signingPassword)
   }
   if (Ci.isRelease) {
      sign(publications)
   }
}

// Create dokka Jar task from dokka task output
//val dokkaJar by tasks.creating(Jar::class) {
//   group = JavaBasePlugin.DOCUMENTATION_GROUP
//   description = "Assembles Kotlin docs with Dokka"
//   archiveClassifier.set("javadoc")
//   from(dokka)
//}

tasks.named<Test>("test") {
   useJUnitPlatform()
   filter {
      isFailOnNoMatchingTests = false
   }
   testLogging {
      showExceptions = true
      showStandardStreams = true
      events = setOf(
         org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED,
         org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED
      )
      exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
   }
}

val javadocJar by tasks.creating(Jar::class) {
   group = JavaBasePlugin.DOCUMENTATION_GROUP
   description = "Assembles java doc to jar"
   archiveClassifier.set("javadoc")
   from(javadoc)
}

java {
   targetCompatibility = org.gradle.api.JavaVersion.VERSION_1_8
   withJavadocJar()
   withSourcesJar()
}

publishing {
   repositories {
      maven {
         val releasesRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
         val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots/")
         name = "deploy"
         url = if (Ci.isRelease) releasesRepoUrl else snapshotsRepoUrl
         credentials {
            username = System.getenv("OSSRH_USERNAME") ?: ossrhUsername
            password = System.getenv("OSSRH_PASSWORD") ?: ossrhPassword
         }
      }
   }

   publications {
      register("mavenJava", MavenPublication::class) {
         from(components["java"])
         artifact(javadocJar)
         pom {
            name.set("avro4k-core")
            description.set("Avro format support for kotlinx.serialization")
            url.set("http://www.github.com/avro-kotlin/avro4k")

            scm {
               connection.set("scm:git:http://www.github.com/avro-kotlin/avro4k")
               developerConnection.set("scm:git:http://github.com/avro-kotlin/avro4k")
               url.set("http://www.github.com/avro-kotlin/avro4k")
            }

            licenses {
               license {
                  name.set("Apache-2.0")
                  url.set("https://opensource.org/licenses/Apache-2.0")
               }
            }

            developers {
               developer {
                  id.set("sksamuel")
                  name.set("Stephen Samuel")
                  email.set("sam@sksamuel.com")
               }
               developer {
                  id.set("thake")
                  name.set("Thorsten Hake")
                  email.set("mail@thorsten-hake.com")
               }
            }
         }
      }
   }
}