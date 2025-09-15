# Avro4k's Gradle Plugin

The Avro4k Gradle plugin simplifies the integration of existing Avro schemas into your Kotlin projects.
It automates the generation of Kotlin data classes from Avro schema files, ensuring that your data models are always in sync with your Avro definitions.
It also ensures the versions are aligned with the plugin version.

If the plugin is applied, there is no need to add the `avro4k-core` dependency manually, as it will be added automatically.

## Setup

To use the Avro4k Gradle plugin, add the following to your `build.gradle.kts` file:

```kotlin
plugins {
    id("io.github.avro-kotlin") version "2.5.3"
}

// Optional: customize the plugin configuration
avro4k {
    sourcesGeneration {
        inputSchemas.from(file("your-specific-schema.avsc"))
        outputDir = file("src/main/generated")
    }
}

dependencies {
    // Add confluent serde if needed
    implementation("com.github.avro-kotlin.avro4k:avro4k-confluent-kafka-serializer") // No need to precise the version as the plugin takes care of it!
}
```

By default, the plugin looks for Avro schema files in `src/main/avro`, generates Kotlin data classes in `build/generated/sources/avro/main`, and plug this sourceSet to the main sourceSet.

> [!NOTE]
> If you want to commit the generated sources, consider setting the `outputDir` to a directory outside of `build/`.

Then, just use your generated classes in your code (the main sourceSet includes the `outputDir` by default):

```kotlin
import com.example.yourpackage.YourGeneratedClass
Avro.encodeToByteArray(YourGeneratedClass(...))
```
