import com.github.avrokotlin.avro4k.NameStrategy

plugins {
    kotlin("jvm") version "2.2.10"
    id("io.github.avro-kotlin") version  "local-SNAPSHOT"
}

repositories {
    mavenCentral()
    mavenLocal()
}

avro4k {
    sourcesGeneration {
        // Demonstrate camelCase Avro fields becoming snake_cased Kotlin properties.
        nameStrategy(NameStrategy.SNAKE_CASE)


        /* Other naming strategies
         nameStrategy(NameStrategy.IDENTITY) // default
         nameStrategy(NameStrategy.CAMEL_CASE)
         nameStrategy(NameStrategy.PASCAL_CASE)
        */

        /* When using a lambda-based custom naming strategy,
         Gradle cannot detect changes in the lambda implementation.
         If the lambda logic is modified, the key must be updated manually
         to invalidate incremental builds and build cache correctly.
         nameStrategy("key-for-gradle-up-to-date") {
             original -> original.reversed()
         }
        */

    }
}
