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
   kotlin("jvm") version Libs.kotlinVersion
   kotlin("plugin.serialization") version Libs.kotlinVersion
   id("maven-publish")
   signing
   id("org.jetbrains.dokka") version Libs.dokkaVersion
   id("io.kotest") version Libs.kotestGradlePlugin
   id("com.github.ben-manes.versions") version Libs.versionsPlugin
   id("io.github.gradle-nexus.publish-plugin") version Libs.nexusPublishPlugin
}

repositories {
   mavenCentral()
   google()
}

tasks {
   javadoc {
   }
}

group = "com.github.avro-kotlin.avro4k"
version = Ci.publishVersion

dependencies {
   api(Libs.Avro.avro)
   api(Libs.Kotlinx.serializationCore)
   implementation(Libs.Kotlinx.serializationJson)
   implementation(kotlin("reflect"))
   implementation(Libs.Xerial.snappy)
   testImplementation(Libs.Kotest.junit5)
   testImplementation(Libs.Kotest.assertionsCore)
   testImplementation(Libs.Kotest.assertionsJson)
   testImplementation(Libs.Kotest.proptest)
}

tasks.withType<KotlinCompile>().configureEach {
   kotlinOptions.jvmTarget = "1.8"
   kotlinOptions.apiVersion = "1.5"
   kotlinOptions.languageVersion = "1.5"
   kotlinOptions.freeCompilerArgs += "-opt-in=kotlin.RequiresOptIn"
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
      events = setOf(
         org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED,
         org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED
      )
      exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
   }
}
tasks.named<Jar>("javadocJar") {
   from(tasks.named("dokkaJavadoc"))
}

//Configuration for publication on maven central
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
            name.set("avro4k-core")
            description.set("Avro format support for kotlinx.serialization")
            url.set("https://www.github.com/avro-kotlin/avro4k")

            scm {
               connection.set("scm:git:https://www.github.com/avro-kotlin/avro4k")
               developerConnection.set("scm:git:https://github.com/avro-kotlin/avro4k")
               url.set("https://www.github.com/avro-kotlin/avro4k")
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
fun Project.publishing(action: PublishingExtension.() -> Unit) =
   configure(action)

fun Project.signing(configure: SigningExtension.() -> Unit): Unit =
   configure(configure)
