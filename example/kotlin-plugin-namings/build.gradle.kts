import com.github.avrokotlin.avro4k.plugin.gradle.FieldNamingStrategyType

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
        fieldNamingStrategy = FieldNamingStrategyType.SNAKE_CASE

        // Other available strategies:
        // fieldNamingStrategy = FieldNamingStrategyType.IDENTITY   // default - keeps original names
        // fieldNamingStrategy = FieldNamingStrategyType.CAMEL_CASE // converts to camelCase
        // fieldNamingStrategy = FieldNamingStrategyType.PASCAL_CASE // converts to PascalCase
    }
}
