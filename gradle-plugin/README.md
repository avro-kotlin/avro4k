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

## Logical Types

The plugin supports customizing how Avro logical types are mapped to Kotlin classes during code generation. This is particularly useful when you want to use specific Kotlin types instead of the default Java types.

### Using Kotlin Native UUID

By default, UUID logical types are generated using `java.util.UUID`. If you prefer to use Kotlin's native `kotlin.uuid.Uuid` instead, you can use the `useKotlinUuid()` convenience method:

```kotlin
avro4k {
    sourcesGeneration {
        useKotlinUuid()
    }
}
```

This will generate code like:

```kotlin
@Serializable
public value class MySchema(
    @Serializable(with = KotlinUuidSerializer::class)
    public val id: Uuid,
)
```

### Custom Logical Type Mappings

You can also configure custom logical type mappings directly using the `logicalTypes` map property:

```kotlin
avro4k {
    sourcesGeneration {
        // Map UUID logical type to Kotlin native UUID
        logicalTypes.put("uuid", "kotlin.uuid.Uuid")

        // Add custom logical type mappings
        logicalTypes.put("my-custom-type", "com.example.MyCustomType")
    }
}
```

The plugin automatically provides serializers for well-known types:
- `uuid` → `kotlin.uuid.Uuid`: Uses `KotlinUuidSerializer`
- `uuid` → `java.util.UUID`: Uses `UUIDSerializer`

For custom types not recognized by the plugin, the generated code will be annotated with `@Contextual`, and you'll need to configure the corresponding serializer in your `Avro` instance using `AvroBuilder.setLogicalTypeSerializer()`.

### Example with Custom Logical Types

```kotlin
avro4k {
    sourcesGeneration {
        // Use Kotlin UUID
        useKotlinUuid()

        // Add your custom logical types
        logicalTypes.put("timestamp-millis", "kotlinx.datetime.Instant")
        logicalTypes.put("my-decimal", "java.math.BigDecimal")
    }
}
```

Don't forget to configure the serializers in your runtime code:

```kotlin
val avro = Avro {
    setLogicalTypeSerializer("my-decimal", BigDecimalSerializer)
    setLogicalTypeSerializer("timestamp-millis", InstantSerializer)
}
```
