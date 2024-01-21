pluginManagement {
   repositories {
      mavenCentral()
      gradlePluginPortal()
   }
}

rootProject.name = "avro4k-core"

dependencyResolutionManagement {
   versionCatalogs {
      create("libs") {
         version("kotlin", "1.8.20")
         version("jvm", "18")

         library("xerial-snappy", "org.xerial.snappy", "snappy-java").version("1.1.10.1")
         library("apache-avro", "org.apache.avro", "avro").version("1.11.3")

         val kotlinxSerialization = "1.5.0"
         library("kotlinx-serialization-core", "org.jetbrains.kotlinx", "kotlinx-serialization-core").version(kotlinxSerialization)
         library("kotlinx-serialization-json", "org.jetbrains.kotlinx", "kotlinx-serialization-json").version(kotlinxSerialization)

         val kotestVersion = "5.6.1"
         library("kotest-core", "io.kotest", "kotest-assertions-core").version(kotestVersion)
         library("kotest-json", "io.kotest", "kotest-assertions-json").version(kotestVersion)
         library("kotest-junit5", "io.kotest", "kotest-runner-junit5").version(kotestVersion)
         library("kotest-property", "io.kotest", "kotest-property").version(kotestVersion)

         plugin("dokka", "org.jetbrains.dokka").version("1.8.10")
         plugin("kotest", "io.kotest").version("0.4.10")
         plugin("github-versions", "com.github.ben-manes.versions").version("0.46.0")
         plugin("nexus-publish", "io.github.gradle-nexus.publish-plugin").version("1.3.0")
      }
   }
   repositories {
      mavenCentral()
   }
}
