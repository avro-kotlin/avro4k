![build-main](https://github.com/avro-kotlin/avro4k/workflows/build-main/badge.svg)
[![Download](https://img.shields.io/maven-central/v/com.github.avro-kotlin.avro4k/avro4k-core)](https://search.maven.org/artifact/com.github.avro-kotlin.avro4k/avro4k-core)
[![Kotlin](https://img.shields.io/badge/kotlin-2.0.0-blue.svg?logo=kotlin)](http://kotlinlang.org)
[![Kotlinx serialization](https://img.shields.io/badge/kotlinx--serialization-1.7.0-blue?logo=kotlin)](https://github.com/Kotlin/kotlinx.serialization)
[![Avro spec](https://img.shields.io/badge/avro%20spec-1.11.3-blue.svg?logo=apache)](https://avro.apache.org/docs/1.11.3/specification/)

# Introduction

**Avro4k** (or Avro for Kotlin) is a library that brings [Avro](https://avro.apache.org/) serialization format in kotlin, based on the **reflection-less** kotlin library
called [kotlinx-serialization](https://github.com/Kotlin/kotlinx.serialization).

Here are the main features:

- **Full avro support**, including logical types, unions, recursive types, and schema evolution :white_check_mark:
- **Encode and decode** anything to and from binary format, and also in generic data :toolbox:
- **Generate schemas** based on your values and data classes :pencil:
- **Customize** the generated schemas and encoded data with annotations :construction_worker:
- **Fast** as it is reflection-less :rocket: (check the benchmarks [here](benchmark/README.md#results))
- **Simple API** to get started quickly, also with native support of java standard classes like `UUID`, `BigDecimal`, `BigInteger` and `java.time` module :1st_place_medal:
- **Relaxed matching** for easy schema evolution as it natively [adapts compatible types](#types-matrix) :cyclone:

> [!WARNING]
> **Important**: As of today, avro4k is **only available for JVM platform**, and theoretically for android platform (as apache avro library is already **android-ready**). <br/>If
> you would like to have js/wasm/native compatible platforms, please put a :thumbsup: on [this issue](https://github.com/avro-kotlin/avro4k/issues/207)

# Quick start

## Basic encoding

<details open>
<summary>Example:</summary>

```kotlin
package myapp

import com.github.avrokotlin.avro4k.*
import kotlinx.serialization.*

@Serializable
data class Project(val name: String, val language: String)

fun main() {
    // Generating schemas
    val schema = Avro.schema<Project>()
    println(schema.toString()) // {"type":"record","name":"Project","namespace":"myapp","fields":[{"name":"name","type":"string"},{"name":"language","type":"string"}]}

    // Serializing objects
    val data = Project("kotlinx.serialization", "Kotlin")
    val bytes = Avro.encodeToByteArray(data)

    // Deserializing objects
    val obj = Avro.decodeFromByteArray<Project>(bytes)
    println(obj) // Project(name=kotlinx.serialization, language=Kotlin)
}
```

</details>

## Single object

Avro4k provides a way to encode and decode single objects with `AvroSingleObject` class. This encoding will prefix the binary data with the schema fingerprint to
allow knowing the writer schema when reading the data. The downside is that you need to provide a schema registry to get the schema from the fingerprint.
This format is perfect for payloads sent through message brokers like kafka or rabbitmq as it is the most compact schema-aware format.

<details>
<summary>Example:</summary>

```kotlin
package myapp

import com.github.avrokotlin.avro4k.*
import kotlinx.serialization.*
import org.apache.avro.SchemaNormalization

@Serializable
data class Project(val name: String, val language: String)

fun main() {
    val schema = Avro.schema<Project>()
    val schemasByFingerprint = mapOf(SchemaNormalization.parsingFingerprint64(schema), schema)
    val singleObjectInstance = AvroSingleObject { schemasByFingerprint[it] }

    // Serializing objects
    val data = Project("kotlinx.serialization", "Kotlin")
    val bytes = singleObjectInstance.encodeToByteArray(data)

    // Deserializing objects
    val obj = singleObjectInstance.decodeFromByteArray<Project>(bytes)
    println(obj) // Project(name=kotlinx.serialization, language=Kotlin)
}
```

</details>

> For more details, check in the avro spec the [single object encoding](https://avro.apache.org/docs/1.11.3/specification/#single-object-encoding).

## Object container

Avro4k provides a way to encode and decode object container — also known as data file — with `AvroObjectContainer` class. This encoding will prefix the binary data with the
full schema to allow knowing the writer schema when reading the data. This format is perfect for storing many long-term objects in a single file.

Be aware that consuming the decoded `Sequence` needs to be done **before** closing the stream, or you will get an exception as a sequence is a "hot" source,
which means that if there is millions of objects in the file, all the objects are extracted one-by-one when requested. If you take only the first 10 objects and close the stream,
the remaining objects won't be extracted. Use carefully `sequence.toList()` as it could lead to OutOfMemoryError as extracting millions of objects may not fit in memory.

<details>
<summary>Example:</summary>

```kotlin
package myapp

import com.github.avrokotlin.avro4k.*
import kotlinx.serialization.*

@Serializable
data class Project(val name: String, val language: String)

fun main() {
    // Serializing objects
    val valuesToEncode = sequenceOf(
        Project("kotlinx.serialization", "Kotlin"),
        Project("java.lang", "Java"),
        Project("avro4k", "Kotlin"),
    )
    Files.newOutputStream(Path("your-file.bin")).use { fileStream ->
        AvroObjectContainer.openWriter(fileStream).use { writer ->
            valuesToEncode.forEach { writer.write(it) }
        }
    }

    // Deserializing objects
    Files.newInputStream(Path("your-file.bin")).use { fileStream ->
        AvroObjectContainer.decodeFromStream<Project>(fileStream).forEach {
            println(it) // Project(name=kotlinx.serialization, language=Kotlin) ...
        }
    }
}
```

</details>

> For more details, see the Avro spec on [object container files](https://avro.apache.org/docs/1.11.3/specification/#object-container-files).

# Important notes

- **Avro4k** is highly based on apache avro library, that implies all the schema validation is done by it
- All members annotated with `@ExperimentalSerializationApi` are **subject to changes** in future releases without any notice as they are experimental, so please
  check the release notes to check the needed migration. At least, given a version `A.B.C`, only the minor `B` number will be incremented, not the major `A`.
- **Avro4k** also supports encoding and decoding generic data, mainly because of confluent schema registry compatibility as their serializers only handle generic data. When avro4k
  will support their schema registry, the generic encoding will be removed to keep this library as simple as possible.

# Setup

<details open>
  <summary><b>Gradle Kotlin DSL</b></summary>

```kotlin
plugins {
    kotlin("jvm") version kotlinVersion
    kotlin("plugin.serialization") version kotlinVersion
}

dependencies {
    implementation("com.github.avro-kotlin.avro4k:avro4k-core:$avro4kVersion")
}
```

</details>

<br>

<details>
  <summary><b>Gradle Groovy DSL</b></summary>

```groovy
plugins {
    id 'org.jetbrains.kotlin.multiplatform' version kotlinVersion
    id 'org.jetbrains.kotlin.plugin.serialization' version kotlinVersion
}

dependencies {
    implementation "com.github.avro-kotlin.avro4k:avro4k-core:$avro4kVersion"
}
```

</details>

<br>

<details>
  <summary><b>Maven</b></summary>

Add serialization plugin to Kotlin compiler plugin:

```xml

<build>
    <plugins>
        <plugin>
            <groupId>org.jetbrains.kotlin</groupId>
            <artifactId>kotlin-maven-plugin</artifactId>
            <version>${kotlin.version}</version>
            <executions>
                <execution>
                    <id>compile</id>
                    <phase>compile</phase>
                    <goals>
                        <goal>compile</goal>
                    </goals>
                </execution>
            </executions>
            <configuration>
                <compilerPlugins>
                    <plugin>kotlinx-serialization</plugin>
                </compilerPlugins>
            </configuration>
            <dependencies>
                <dependency>
                    <groupId>org.jetbrains.kotlin</groupId>
                    <artifactId>kotlin-maven-serialization</artifactId>
                    <version>${kotlin.version}</version>
                </dependency>
            </dependencies>
        </plugin>
    </plugins>
</build>
```

Add the avro4k dependency:

```xml

<dependency>
    <groupId>com.github.avro-kotlin.avro4k</groupId>
    <artifactId>avro4k-core</artifactId>
    <version>${avro4k.version}</version>
</dependency>
```

</details>

## Versions matrix

| Avro4k     | Kotlin   | Kotlin API/language | Kotlin serialization |
|------------|----------|---------------------|----------------------|
| `>= 2.0.0` | `>= 2.0` | `>= 1.9`            | `>= 1.7`             |
| `< 2.0.0`  | `>= 1.6` | `>= 1.6`            | `>= 1.3`             |

# How to generate schemas

Writing schemas manually or using the Java based `SchemaBuilder` can be tedious.
`kotlinx-serialization` simplifies this generating for us the corresponding descriptors to allow generating avro schemas easily, without any reflection.
Also, it provides native compatibility with data classes (including open and sealed classes), inline classes, any collection, array, enums, and primitive values.

> [!NOTE]
> For more information about the avro schema, please refer to the [avro specification](https://avro.apache.org/docs/1.11.3/specification/)

To allow generating a schema for a specific class, you need to annotate it with `@Serializable`:

```kotlin
@Serializable
data class Ingredient(val name: String, val sugar: Double)

@Serializable
data class Pizza(val name: String, val ingredients: List<Ingredient>, val topping: Ingredient?, val vegetarian: Boolean)
```

Then you can generate the schema using the `Avro.schema` function:

```kotlin
val schema = Avro.schema<Pizza>()
println(schema.toString(true))
```

The generated schema will look as follows:

```json
{
    "type": "record",
    "name": "Pizza",
    "namespace": "com.github.avrokotlin.avro4k.example",
    "fields": [
        {
            "name": "name",
            "type": "string"
        },
        {
            "name": "ingredients",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Ingredient",
                    "fields": [
                        {
                            "name": "name",
                            "type": "string"
                        },
                        {
                            "name": "sugar",
                            "type": "double"
                        }
                    ]
                }
            }
        },
        {
            "name": "topping",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Ingredient"
                }
            ],
            "default": null
        },
        {
            "name": "vegetarian",
            "type": "boolean"
        }
    ]
}
```

If you need to configure your `Avro` instance, you need to create your own instance of `Avro` with the wanted configuration, and then use it to generate the schema:

```kotlin
val yourAvroInstance = Avro {
    // your configuration
}
yourAvroInstance.schema<Pizza>()
```

# Usage

## Customizing the configuration

By default, `Avro` is configured with the following behavior:
- `implicitNulls`: The nullable fields are considered null when decoding if the writer record's schema does not contain this field.
- `implicitEmptyCollections`: The non-nullable map and collection fields are considered empty when decoding if the writer record's schema does not contain this field.
  - If `implicitNulls` is true, it takes precedence so the empty collections are set as null if the value is missing instead of an empty collection.
- `validateSerialization`: There is no validation of the schema when encoding or decoding data, which means that serializing using a custom serializer could lead to unexpected behavior. Be careful with your custom serializers. More details [in this section](#set-a-custom-schema).
- `fieldNamingStrategy`: The record's field naming strategy is using the original kotlin field name. To change it, [check this section](#changing-records-field-name).

So each time you call a method on the `Avro` object implicitely invoke the default configuration. Example:

```kotlin
Avro.encodeToByteArray(MyData("value"))
Avro.decodeFromByteArray(bytes)
Avro.schema<MyData>()
```

If you need to change the default behavior, you need to create your own instance of `Avro` with the wanted configuration:

```kotlin
val yourAvroInstance = Avro {
    fieldNamingStrategy = FieldNamingStrategy.Builtins.SnakeCase
    implicitNulls = false
    implicitEmptyCollections = false
    validateSerialization = true
}
yourAvroInstance.encodeToByteArray(MyData("value"))
yourAvroInstance.decodeFromByteArray(bytes)
yourAvroInstance.schema<MyData>()
```

## Types matrix

| Kotlin type                  | Generated schema type | Other compatible writer types                                | Compatible logical type | Note / Serializer class                                                                                                                             |
|------------------------------|-----------------------|--------------------------------------------------------------|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `Boolean`                    | `boolean`             | `string`                                                     |                         |                                                                                                                                                     |
| `Byte`, `Short`, `Int`       | `int`                 | `long`, `float`, `double`, `string`                          |                         |                                                                                                                                                     |
| `Long`                       | `long`                | `int`, `float`, `double`, `string`                           |                         |                                                                                                                                                     |
| `Float`                      | `float`               | `double`, `string`                                           |                         |                                                                                                                                                     |
| `Double`                     | `double`              | `float`, `string`                                            |                         |                                                                                                                                                     |
| `Char`                       | `int`                 | `string` (exactly 1 char required)                           | `char`                  | The value serialized is the char code. When reading from a `string`, requires exactly 1 char                                                        |
| `String`                     | `string`              | `bytes` (UTF8), `fixed` (UTF8)                               |                         |                                                                                                                                                     |
| `ByteArray`                  | `bytes`               | `string` (UTF8), `fixed` (UTF8)                              |                         |                                                                                                                                                     |
| `Map<*, *>`                  | `map`                 |                                                              |                         | The map key must be string-able. Mainly everything is string-able except null and composite types (collection, data classes)                        |
| `Collection<*>`              | `array`               |                                                              |                         |                                                                                                                                                     |
| `data class`                 | `record`              |                                                              |                         |                                                                                                                                                     |
| `enum class`                 | `enum`                | `string`                                                     |                         |                                                                                                                                                     |
| `@AvroFixed`-compatible      | `fixed`               | `bytes`, `string`                                            |                         | Throws an error at runtime if the writer type is not present in the column "other compatible writer types"                                          |
| `@AvroStringable`-compatible | `string`              | `int`, `long`, `float`, `double`, `string`, `fixed`, `bytes` |                         | Ignored when the writer type is not present in the column "other compatible writer types"                                                           |
| `java.math.BigDecimal`       | `bytes`               | `int`, `long`, `float`, `double`, `string`, `fixed`, `bytes` | `decimal`               | To use it, annotate the field with `@AvroDecimal` to give the `scale` and the `precision`                                                           |
| `java.math.BigDecimal`       | `string`              | `int`, `long`, `float`, `double`, `fixed`, `bytes`           |                         | To use it, annotate the field with `@AvroStringable`. `@AvroDecimal` is ignored in that case                                                        |
| `java.util.UUID`             | `string`              |                                                              | `uuid`                  | To use it, just annotate the field with `@Contextual`                                                                                               |
| `java.net.URL`               | `string`              |                                                              |                         | To use it, just annotate the field with `@Contextual`                                                                                               |
| `java.math.BigInteger`       | `string`              | `int`, `long`, `float`, `double`                             |                         | To use it, just annotate the field with `@Contextual`                                                                                               |
| `java.time.LocalDate`        | `int`                 | `long`, `string` (ISO8601)                                   | `date`                  | To use it, just annotate the field with `@Contextual`                                                                                               |
| `java.time.Instant`          | `long`                | `string` (ISO8601)                                           | `timestamp-millis`      | To use it, just annotate the field with `@Contextual`                                                                                               |
| `java.time.Instant`          | `long`                | `string` (ISO8601)                                           | `timestamp-micros`      | To use it, [register the serializer](#support-additional-non-serializable-types) `com.github.avrokotlin.avro4k.serializer.InstantToMicroSerializer` |
| `java.time.LocalDateTime`    | `long`                | `string` (ISO8601)                                           | `timestamp-millis`      | To use it, just annotate the field with `@Contextual`                                                                                               |
| `java.time.LocalTime`        | `int`                 | `long`, `string` (ISO8601)                                   | `time-millis`           | To use it, just annotate the field with `@Contextual`                                                                                               |
| `java.time.Duration`         | `fixed` of 12         | `string` (ISO8601)                                           | `duration`              | To use it, just annotate the field with `@Contextual`                                                                                               |
| `java.time.Period`           | `fixed` of 12         | `string` (ISO8601)                                           | `duration`              | To use it, just annotate the field with `@Contextual`                                                                                               |
| `kotlin.time.Duration`       | `fixed` of 12         | `string` (ISO8601)                                           | `duration`              |                                                                                                                                                     |

> [!NOTE]
> For more details, check the [built-in classes in kotlinx-serialization](https://github.com/Kotlin/kotlinx.serialization/blob/master/docs/builtin-classes.md)

## Add documentation to a schema

You may want to add documentation to a schema to provide more information about a field or a named type (only RECORD and ENUM for the moment).

> [!WARNING]
> Do not use `@org.apache.avro.reflect.AvroDoc` as this annotation is not visible by Avro4k.

```kotlin
import com.github.avrokotlin.avro4k.AvroDoc

@Serializable
@AvroDoc("This is a record documentation")
data class MyData(
    @AvroDoc("This is a field documentation")
    val myField: String
)

@Serializable
@AvroDoc("This is an enum documentation")
enum class MyEnum {
    A,
    B
}
```

> [!NOTE]
> This impacts only the schema generation.

## Support additional non-serializable types

When looking at the [types matrix](#types-matrix), you can see some of them natively supported by Avro4k, but some others are not.
Also, your own types may not be serializable.

To fix it, you need to create a custom **serializer** that will handle the serialization and deserialization of the value, and provide a descriptor.

> [!NOTE]
> This impacts the serialization and the deserialization. It can also impact the schema generation if the serializer is providing a custom logical type or a custom schema through
> the descriptor.

### Write your own serializer

To create a custom serializer, you need to implement the `AvroSerializer` abstract class and override the `serializeAvro` and `deserializeAvro` methods.
You also need to override `getSchema` to provide the schema of your custom type as a custom serializer means non-standard encoding and decoding.

<details open>
<summary>Create a serializer that needs Avro features like getting the schema or encoding bytes and fixed types</summary>

```kotlin
object YourTypeSerializer : AvroSerializer<YourType>(YourType::class.qualifiedName!!) {
    override fun getSchema(context: SchemaSupplierContext): Schema {
        // you can access the data class element, inlined elements from value classes, and their annotations
        // you can also access the avro configuration in the context
        return ... /* create the corresponding schema using SchemaBuilder or Schema.create */
    }

    override fun serializeAvro(encoder: AvroEncoder, value: YourType) {
        encoder.currentWriterSchema // you can access the current writer schema
        encoder.encodeString(value.toString())
    }

    override fun deserializeAvro(decoder: AvroDecoder): YourType {
        decoder.currentWriterSchema // you can access the current writer schema
        return YourType.fromString(decoder.decodeString())
    }

    override fun serializeGeneric(encoder: Encoder, value: YourType) {
        // you may want to implement this function if you also want to use the serializer outside of Avro4k
        encoder.encodeString(value.toString())
    }

    override fun deserializeGeneric(decoder: Decoder): YourType {
        // you may want to implement this function if you also want to use the serializer outside of Avro4k
        return YourType.fromString(decoder.decodeString())
    }
}
```

</details>

You may want to just implement a `KSerializer` if you don't need specific Avro features, but you won't be able to associate a custom schema to it:

<details>
<summary>Create a generic serializer that doesn't need specific Avro features</summary>

```kotlin
object YourTypeSerializer : KSerializer<YourType> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("YourType", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: YourType) {
        encoder.encodeString(value.toString())
    }

    override fun deserialize(decoder: Decoder): YourType {
        return YourType.fromString(decoder.decodeString())
    }
}
```

</details>

### Register the serializer globally (not compile time)

You first need to configure your `Avro` instance with the wanted serializer instance:

```kotlin
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual

val myCustomizedAvroInstance = Avro {
    serializersModule = SerializersModule {
        // give the object serializer instance
        contextual(YourTypeSerializerObject)
        // or instanciate it if it's a class and not an object
        contextual(YourTypeSerializerClass())
    }
}
```

Then just annotated the field with `@Contextual`:

```kotlin
@Serializable
data class MyData(
    @Contextual val myField: YourType
)
```

### Register the serializer just for a field at compile time

```kotlin
@Serializable
data class MyData(
    @Serializable(with = YourTypeSerializer::class) val myField: YourType
)
```

## Changing record's field name

By default, field names are the original name of the kotlin fields in the data classes.

> [!NOTE]
> This impacts the schema generation, the serialization and the deserialization of the field.

### Individual field name change

To change a field name, annotate it with `@SerialName`:

```kotlin
@Serializable
data class MyData(
    @SerialName("custom_field_name") val myField: String
)
```

> [!NOTE]
> `@SerialName` will still be handled by the naming strategy

### Field naming strategy (overall change)

To apply a naming strategy to all fields, you need to set the `fieldNamingStrategy` in the `Avro` configuration.

> [!NOTE]
> This is only applicable for RECORD fields, and not for ENUM symbols.

There is 3 built-ins strategies:

- `NoOp` (default): keeps the original kotlin field name
- `SnakeCase`: converts the original kotlin field name to snake_case with underscores before each uppercase letter
- `PascalCase`: upper-case the first letter of the original kotlin field name
- If you need more, please [file an issue](https://github.com/avro-kotlin/avro4k/issues/new/choose)

First, create your own instance of `Avro` with the wanted naming strategy:

```kotlin

val myCustomizedAvroInstance = Avro {
    fieldNamingStrategy = FieldNamingStrategy.Builtins.SnakeCase
}

```

Then, use this instance to generate the schema or encode/decode data:

```kotlin
package my.package

@Serializable
data class MyData(val myField: String)

val schema = myCustomizedAvroInstance.schema<MyData>() // {...,"fields":[{"name":"my_field",...}]}
```

## Set a default field value

While reading avro binary data, you can miss a field (a kotlin field is present but not in the avro binary data), so Avro4k fails as it is not capable of constructing the kotlin
type without the missing field value.

By default:
- nullable fields are optional and `default: null` is automatically added to the field ([check this section](#disable-implicit-default-null-for-nullable-fields) to opt out from this default behavior).

### @AvroDefault

To avoid this error, you can set a default value for a field by annotating it with `@AvroDefault`:

```kotlin
import com.github.avrokotlin.avro4k.AvroDefault

@Serializable
data class MyData(
    @AvroDefault("default value") val stringField: String,
    @AvroDefault("42") val intField: Int?,
    @AvroDefault("""{"stringField":"custom value"}""") val nestedType: MyData? = null
)
```

> [!NOTE]
> This impacts only the schema generation and the deserialization of the field, and not the serialization.

> [!WARNING]
> Do not use `@org.apache.avro.reflect.AvroDefault` as this annotation is not visible by Avro4k.

### kotlin default value

You can also set a kotlin default value, but this default won't be present into the generated schema as Avro4k is not able to retrieve it:

```kotlin
@Serializable
data class MyData(
    val stringField: String = "default value",
    val intField: Int? = 42,
)
```

> This impacts only the deserialization of the field, and not the serialization or the schema generation.

## Add aliases

To be able of reading from different written schemas, or able of writing to different schemas, you can add aliases to a named type (record, enum) field by annotating it
with `@AvroAlias`. The given aliases may contain the full name of the alias type or only the name.

> [Avro spec link](https://avro.apache.org/docs/1.11.3/specification/#aliases)

> [!NOTE]
> Aliases are not impacted by [naming strategy](#field-naming-strategy-overall-change), so you need to provide aliases directly applying the corresponding naming strategy if you
> need to respect it.

```kotlin
import com.github.avrokotlin.avro4k.AvroAlias

@Serializable
@AvroAlias("full.name.RecordName", "JustOtherRecordName")
data class MyData(
    @AvroAlias("anotherFieldName", "old_field_name") val myField: String
)
```

> [!NOTE]
> This impacts the schema generation, the serialization and the deserialization.

> [!WARNING]
> Do not use `@org.apache.avro.reflect.AvroAlias` as this annotation is not visible by Avro4k.

## Add metadata to a schema (custom properties)

You can add custom properties to a schema to have additional metadata on a type.
To do so, you can annotate the data class or field with `@AvroProp`. The value can be a regular string or any json content:

```kotlin
@Serializable
@AvroProp("custom_string_property", "The default non-json value")
@AvroProp("custom_int_property", "42")
@AvroProp("custom_json_property", """{"key":"value"}""")
data class MyData(
    @AvroProp("custom_field_property", "Also working on fields")
    val myField: String
)
```

To add metadata to a type not owned by you, you can use a value class. Here an example with a `BigQuery` type that needs the property `sqlType = JSON` on `string` type:
```kotlin
@Serializable
value class BigQueryJson(@AvroProp("sqlType", "JSON") val value: String)

println(Avro.schema<BigQueryJson>().toString(true)) // {"type":"string","sqlType":"JSON"}
```

> [!NOTE]
> This impacts only the schema generation. For more details, check the [avro specification](https://avro.apache.org/docs/1.11.3/specification/#schema_props).

> [!WARNING]
> Do not use `@org.apache.avro.reflect.AvroMeta` as this annotation is not visible by Avro4k.

## Change scale and precision for `decimal` logical type

By default, the scale is `2` and the precision `8`. To change it, annotate the field with `@AvroDecimal`:

```kotlin
@Serializable
data class MyData(
    @AvroDecimal(scale = 4, precision = 10) val myField: BigDecimal
)
```

> [!NOTE]
> This impacts the schema generation, the serialization and the deserialization.

## Change enum values' name

By default, enum symbols are exactly the name of the enum values in the enum classes. To change this default, you need to annotate enum values with `@SerialName`.

```kotlin
@Serializable
enum class MyEnum {
    @SerialName("CUSTOM_NAME")
    A,
    B,
    C
}
```

> [!NOTE]
> This impacts the schema generation, the serialization and the deserialization.

## Set enum default

When reading with a schema but was written with a different schema, sometimes the reader can miss the enum symbol that triggers an error.
To avoid this error, you can set a default symbol for an enum by annotating the expected fallback with `@AvroEnumDefault`.

```kotlin
@Serializable
enum class MyEnum {
    A,

    @AvroEnumDefault
    B,

    C
}
```

> [!NOTE]
> This impacts the schema generation, the serialization and the deserialization.

## Change type name (RECORD and ENUM)

RECORD and ENUM types in Avro have a name and a namespace (composing a full-name like `namespace.name`). By default, the name is the name of the class/enum and the namespace is the
package name.
To change this default, you need to annotate data classes and enums with `@SerialName`.

> [!WARNING]
> `@SerialName` is redefining the full-name of the annotated class or enum, so you **must** repeat the name or the namespace if you only need to change the namespace or the name
> respectively.

> [!NOTE]
> This impacts the schema generation, the serialization and the deserialization.

### Changing the name while keeping the namespace

```kotlin
package my.package

@Serializable
@SerialName("my.package.MyRecord")
data class MyData(val myField: String)
```

### Changing the namespace while keeping the name

```kotlin
package my.package

@Serializable
@SerialName("custom.namespace.MyData")
data class MyData(val myField: String)
```

### Changing the name and the namespace

```kotlin
package my.package

@Serializable
@SerialName("custom.namespace.MyRecord")
data class MyData(val myField: String)
```

## Change type name (FIXED only)

> [!WARNING]
> For the moment, it is not possible to manually change the namespace or the name of a FIXED type as the type name is coming from the field name and the namespace from the
> enclosing data class package.

## Set a custom schema

To associate a type or a field to a custom schema, you need to create a serializer that will handle the serialization and deserialization of the value, and provide the expected schema.

See [support additional non-serializable types](#support-additional-non-serializable-types) section to get detailed explanation about writing a serializer and
registering it.

## Skip a kotlin field

To skip a field during encoding, you can annotate it with `@kotlinx.serialization.Transient`.
Note that you need to provide a default value for the field as the field will be totally discarded also during encoding (IntelliJ should trigger a warn).

```kotlin
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient

@Serializable
data class Foo(val a: String, @Transient val b: String = "default value")
```

> [!NOTE]
> This impacts the schema generation, the serialization and the deserialization.

## Force a field to be a `string` type

You can force a field (or the value class' property) to have its inferred schema as a `string` type by annotating it with `@AvroString`.

Compatible types visible in the [types matrix](#types-matrix), indicated by the "Other compatible writer types" column. The **writer schema compatibility is still respected**, so if the field has been written as an int, a stringified int will be deserialized as an int without the need of parsing it. It is the same for the rerverse: If an int has been written as a string, it will be deserialized as an int by parsing the string content.

> [!INFO]
> Note that the type must be compatible with the `string` type, otherwise it will be ignored.
> Your custom serializer generated schema must handle this annotation, or it will be ignored.

**Examples:**
```kotlin
@Serializable
data class MyData(
    @AvroString val anInt: Int,
    @AvroString val rawString: ByteArray,
    @AvroString @Contextual val bigDecimal: BigDecimal,
)
@JvmInline
@Serializable
value class StringifiedPrice(
    @AvroString val amount: Double,
)
```

> [!NOTE]
> This impacts the schema generation, the serialization and the deserialization.

# Nullable fields, optional fields and compatibility

With avro, you can have nullable fields and optional fields, that are taken into account for compatibility checking when using the schema registry.

But if you want to remove a nullable field that is not optional, depending on the compatibility mode, it may not be compatible because of the missing default value.

- What is an optional field ?

> An optional field is a field that have a *default* value, like an int with a default as `-1`.

- What is a nullable field ?

> A nullable field is a field that contains a `null` type in its type union, but **it's not an optional field if you don't put `default` value to `null`**.

So to mark a field as optional and facilitate avro contract evolution regarding compatibility checks, then set `default` to `null`.

# Known problems

- Kotlin 1.7.20 up to 1.8.10 cannot properly compile @SerialInfo-Annotations on enums (see https://github.com/Kotlin/kotlinx.serialization/issues/2121).
  This is fixed with kotlin 1.8.20. So if you are planning to use any of avro4k's annotations on enum types, please make sure that you are using kotlin >= 1.8.20.

# Migrating from v1 to v2
Heads up to the [migration guide](Migrating-from-v1.md) to update your code from avro4k v1 to v2.

# Contributions

Contributions to avro4k are always welcome. Good ways to contribute include:

- Raising bugs and feature requests
- Fixing bugs and enhancing the API
- Improving the performance of avro4k
- Adding documentation
