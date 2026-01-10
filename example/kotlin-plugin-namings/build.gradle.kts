import com.github.avrokotlin.avro4k.plugin.gradle.FieldNamingStrategyType

plugins {
    kotlin("jvm") version "2.2.10"
    id("io.github.avro-kotlin")
}

repositories {
    mavenCentral()
    mavenLocal()
}

avro4k {
    sourcesGeneration {
        fieldNamingStrategy = FieldNamingStrategyType.SNAKE_CASE
    }
}
