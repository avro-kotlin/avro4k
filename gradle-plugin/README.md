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

## Code generation features

### Never lose the original schema
Each generated class contains the original schema. It is then used directly by the `Avro` instance to bypass the schema inference,
thus ensuring the original schema from the file to be used during serialization, especially required when using a schema registry afterward.

> [!NOTE]
> The schema content is a normalized schema json content, so it keeps everything but the formatting of the json

### Root types
One of the most challenging part is to keep the original schema even on non-owned or non-generate code (for primitive types like a `String` or an `Int`, logical types, and more...),
while ensuring no deviation between the original schema and the inferred schema (like a `record` for data classes, `int` for a `kotlin.Byte`, or even if you have a particular `java-class` to be used).

Let's take a concrete example:
You have decided with your architect that the schema has to be the following `{"type": "long", "logicalType": "timestamp-micros" }`. So you generate the code but without linking the actual schema, ending up using a standalone `java.time.Instant`.
Then you encode a timestamp, now, with avro4k: `Avro.encodeToByteArray(Instant.now())`.
What is the precision, milliseconds or microseconds? You can specify the schema `Avro.encodeToByteArray(schema, Instant.now())`, by targetting the json schema, bundled into your app.
But what if you have tens of schemas to use in your app? Maintenance becomes a bit more complicated and time-consuming, especially for those standalone types.
That's why generating a root value class helps a lot, by memorizing the original schema, thus having a single source of truth.
Everyone wins: inter-application communication and files management can be done in a language-agnostic manner (e.g. the avro schema) while ensuring type-safety in your kotlin code.


To ensure not losing the original schema from where the code has been generated, the plugin generates a value class wrapping the standalone type,
to be then used without specifying the schema. This applies for:
- any type with a `logicalType` (native to avro4k, or registered in the plugin as contextual or with a concrete serializer)
- any type with a `java-class` (will be marked as contextual)
- any primitive type (`int`, `long`, `float`, `double`, `string`, `bytes`)
- collections (`map`, `array`)
- `fixed` type

Also, nullable types, or a union only containing `null` and the (non-null) type, is generating a value class where its field is basically nullable, without a sealed class.

The value class' name will be based on the file name, formatted in UpperCamelCase without a package (skipping non-word letters).

### Unions as sealed interfaces
Avro is really permissive in the composition of a `union` type, as you could mix by example `null` type, non-compatible primitives (int and string), and records.

In java, it has been solved by using :drums_roll: ... `Òbject` (`Any` in kotlin)! This is the opposite of type-safety.

With the plugin, it will generate for you a sealed interface, where each implementation is basically a value class holding the one of the type in the union.

However, it won't generate a sealed interface with the implementation being directly the data classes (without the value class level), as each type in the union could be used outside the union.

The sealed interface's name will be based on the file name, formatted in UpperCamelCase without a package (skipping non-word letters).

### Schema references (type reusability)
When having many schemas, you may want to have type definitions in shared files, that way any change in them will impact all the schemas referencing those shared types.

It's a native feature of the apache's avro project. So it is able to use references, unless you provide it in the `inputSchemas`.

It needs the schemas referencing a type outside the file to be specified as a string with its full-name.

> [!WARNING]
> Any missing reference will fail the overall code generation!

Here is an example of the schema definition:
- `schemas/Profile.avsc`
```json
{
    "type": "record",
    "name": "Profile",
    "fields": [
        {
            "name": "nickname",
            "type": "string"
        },
        {
            "name": "country",
            "type": "shared.Country"
        }
    ]
}
```
- `schemas/Country.avsc`
```json
{
    "type": "enum",
    "name": "Country",
    "namespace": "shared",
    "symbols": ["FR", "GB", "IT"]
}
```

### Logical types
TODO: 
