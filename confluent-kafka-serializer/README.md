# confluent-kafka-serializer
A kotlin library for serializing and deserializing Kafka messages using Confluent's Schema Registry.

> [!WARNING]
> This library is in early development, so the API may change in future releases.

## Serialization
It can serialize any type which is natively supported by Avro4k, plus:
- Serializing any `java.lang.Collection` and `java.lang.Map` implementation
- Can serialize a `Map` as a `RECORD` schema, where keys are field names and values are field values
- Serializing all the classes implementing:
  * `IndexedRecord`
  * `GenericRecord`
  * `SpecificRecord` and `SpecificRecordBase` (so finally works with generated classes)
  * `GenericEnumSymbol`
  * `GenericFixed`
  * `GenericArray`
- Serializing all the generic types from the apache's avro library:
  * `GenericData.Record`
  * `GenericData.Array`
  * `GenericData.Fixed`
  * `GenericData.EnumSymbol`
  * `ByteBuffer`
  * `Utf8`
- Also supports `NonRecordContainer` to explicit the schema to be used for non-generic types (e.g. types not implementing `GenericContainer`) like primitive types, or specifying unions for schema registration.

> There is no difference between generic serialization and reflection serialization

## Deserialization
First, the logical type is resolved if it exists based on the registered logical types in the Avro instance using `Avro { setLogicalTypeSerializer("logical type name", TheSerializer()) }`.
By default, those logical types are registered:
- `duration` as [com.github.avrokotlin.avro4k.serializer.AvroDuration]
- `uuid` as [java.util.UUID]
- `date` as [java.time.LocalDate]
- `time-millis` as [java.time.LocalTime]
- `time-micros` as [java.time.LocalTime]
- `timestamp-millis` as [java.time.Instant]
- `timestamp-micros` as [java.time.Instant]

Then, for generic deserialization, if there isn't any logical type indicated, or the one set in the schema is not registered, it can deserialize following schema types:
- schema `BOOLEAN`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `STRING` are deserialized as their corresponding kotlin types `Boolean`, `Int`, `Long`, `Float`, `Double`, `String`
- schema `BYTES` is deserialized as `ByteArray`
- schema `ARRAY` is deserialized as `ArrayList`
- schema `MAP` is deserialized as `HashMap`, where keys are always `String`
- schema `FIXED` is deserialized as `GenericFixed`
- schema `ENUM` is deserialized as `GenericEnumSymbol`
- schema `RECORD` is deserialized as `GenericRecord`

For reflection deserialization, it will be a little bit more specific:
- First, if it exists, it deserializes to the type specified in the `java-class` property in the schema. This step is skipped if the property value doesn't exist in the classpath.
- Then, for the named types (`ENUM`, `RECORD`, `FIXED`), it will try to deserialize to a concrete class, where the class lookup is based on the schema full name or its aliases.
- Finally, if no concrete class is found for both `java-class` property and schema name/aliases, it will fallback to generic deserialization.

> [!NOTE]
> **Important note:** To allow deserializing to a type based on its @AvroAlias, or based on a type with a custom @SerialName, you need to register the type to the Avro instance in the SerializersModule

# Usage

Add the dependency to your project:
```kotlin
implementation("com.github.avro-kotlin.avro4k:avro4k-confluent-kafka-serializer:<latest-version>")
```

There are 3 types of serialization:
- Generic: for serializing any type and deserializing to generic classes (like `GenericRecord`, `GenericEnumSymbol`, `GenericFixed`, `GenericArray`) and primitive types (like `Int`, `String`, etc)
- Reflect: for serializing any type but deserializing to specific records or kotlin/java classes. It will fall back to generic deserialization if no specific class is found
- Specific: for serializing (any type) and deserializing to a specific type known at compile time.

## Reflect

If you need to (de)serialize data in kafka based on a schema registry, you would mostly use reflect serdes/serializers/deserializers to allow deserializing concrete kotlin/java classes using reflection.

First, you have to create an instance of the serde. Don't forget to call `.configure(props, isKey)` before using it to configure the schema registry client.
```kotlin
val serde = ReflectAvro4kKafkaSerde()
serde.configure(mapOf("schema.registry.url" to "http://the-url.com"), isKey = false)
```

You can also create a configured instance directly, removing the need to call `.configure`:
```kotlin
val serde = ReflectAvro4kKafkaSerde(
    isKey = false,
    props = mapOf("schema.registry.url" to "http://the-url.com")
)
```

Then, you can get the serializer and deserializer from the serde:
```kotlin
val serializer = serde.serializer()
val deserializer = serde.deserializer()
```

For some use cases like with KafkaProducer or KafkaConsumer, you can also create instances of the serializer and deserializer directly.
```kotlin
val serializer = ReflectAvro4kKafkaSerializer()
serializer.configure(mapOf("schema.registry.url" to "http://the-url.com"), isKey = false)
val deserializer = ReflectAvro4kKafkaDeserializer()
deserializer.configure(mapOf("schema.registry.url" to "http://the-url.com"), isKey = false)
```
Or create configured instances directly:
```kotlin
val serializer = ReflectAvro4kKafkaSerializer(
    isKey = false,
    props = mapOf("schema.registry.url" to "http://the-url.com")
)
val deserializer = ReflectAvro4kKafkaDeserializer(
    isKey = false,
    props = mapOf("schema.registry.url" to "http://the-url.com")
)
```

Finally, if you want to use a custom `Avro` instance (for example, to register custom logical types or a serializers module), you can pass it as a parameter:
```kotlin
val avro = Avro {
    // your custom configuration
    setLogicalTypeSerializer("my-logical-type", MyLogicalTypeSerializer())
    implicitNulls = false
}
val serde = ReflectAvro4kKafkaSerde(
    avro = avro,
    isKey = false,
    props = mapOf("schema.registry.url" to "http://the-url.com")
)
```

## Generic

You will generally use generic (de)serialization when you don't want to instantiate concrete kotlin/java classes during deserialization.

All the creation methods and their usage are similar to the [reflect](#reflect) ones.

## Specific

You can create an instance by passing explicitly the type serializer:
Don't forget to call `.configure(props, isKey)` before using it to configure the schema registry client.
```kotlin
val serde = SpecificAvro4kKafkaSerde(YourType.serializer())
serde.configure(mapOf("schema.registry.url" to "http://the-url.com"), isKey = false)

val serializer = SpecificAvro4kKafkaSerializer(YourType.serializer())
serializer.configure(mapOf("schema.registry.url" to "http://the-url.com"), isKey = false)

val deserializer = SpecificAvro4kKafkaDeserializer(YourType.serializer())
deserializer.configure(mapOf("schema.registry.url" to "http://the-url.com"), isKey = false)
```

You can create an instance using those convenient reified methods to infer the type automatically (done at compile time).
Don't forget to call `.configure(props, isKey)` before using it to configure the schema registry client.
```kotlin
val serde = SpecificAvro4kKafkaSerde<YourType>()
serde.configure(mapOf("schema.registry.url" to "http://the-url.com"), isKey = false)

val serializer = SpecificAvro4kKafkaSerializer<YourType>()
serializer.configure(mapOf("schema.registry.url" to "http://the-url.com"), isKey = false)

val deserializer = SpecificAvro4kKafkaDeserializer<YourType>()
deserializer.configure(mapOf("schema.registry.url" to "http://the-url.com"), isKey = false)
```

Finally, you can directly create configured instances:
```kotlin
val serde = SpecificAvro4kKafkaSerde<YourType>(
    isKey = false,
    props = mapOf("schema.registry.url" to "http://the-url.com")
)

val serializer = SpecificAvro4kKafkaSerializer<YourType>(
    isKey = false,
    props = mapOf("schema.registry.url" to "http://the-url.com")
)

val deserializer = SpecificAvro4kKafkaDeserializer<YourType>(
    isKey = false,
    props = mapOf("schema.registry.url" to "http://the-url.com")
)
```

In any case, if you want to use a custom `Avro` instance (for example, to register custom logical types or a serializers module), you can pass it as a parameter:
```kotlin
val avro = Avro {
    // your custom configuration
}
val serde = SpecificAvro4kKafkaSerde<YourType>(
    avro = avro,
    isKey = false,
    props = mapOf("schema.registry.url" to "http://the-url.com")
)
```

## Spring cloud

> [!NOTE]
> Reference documentation: [Record serialization and deserialization](https://docs.spring.io/spring-cloud-stream/reference/kafka/kafka-streams-binder/record-serialization-and-deserialization.html)

Then, configure the `KafkaAvroSerializer` and `KafkaAvroDeserializer` in your spring configuration:
- for serializing anything and deserializing generic records, enums and fixed types (not using reflection):
  - `com.github.avrokotlin.avro4k.kafka.confluent.GenericAvro4kKafkaSerde`
  - `com.github.avrokotlin.avro4k.kafka.confluent.GenericAvro4kKafkaSerializer`
  - `com.github.avrokotlin.avro4k.kafka.confluent.GenericAvro4kKafkaDeserializer`
- for serializing anything but deserializing to specific records or kotlin/java classes:
    - `com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaSerde`
    - `com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaSerializer`
    - `com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaDeserializer`

```kotlin
spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde: com.github.avrokotlin.avro4k.kafka.confluent.GenericAvro4kKafkaSerde
spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde: com.github.avrokotlin.avro4k.kafka.confluent.GenericAvro4kKafkaSerde

spring.cloud.stream.kafka.streams.bindings.<binder name>.consumer.keySerde: com.github.avrokotlin.avro4k.kafka.confluent.GenericAvro4kKafkaSerde
spring.cloud.stream.kafka.streams.bindings.<binder name>.consumer.valueSerde: com.github.avrokotlin.avro4k.kafka.confluent.GenericAvro4kKafkaSerde
```

## Deserializing logical types

You may need special handling for your own logical types, so that you can deserialize them to specific types instead of the default ones (like `String` for `uuid`).

You can register your own logical type serializers in the `Avro` instance used by the serde/serializer/deserializer.

This applies to all the serialization types: generic, reflect and specific.

```kotlin
val serde = ReflectAvro4kKafkaSerde(
    avro = Avro {
        setLogicalTypeSerializer("my-logical-type", MyLogicalTypeSerializer())
    },
    isKey = false,
    props = mapOf("schema.registry.url" to "http://the-url.com")
)
```

## Deserializing unknown schema names (aliases, custom names)

When using a schema registry, you are probably going to evolve your schemas and models.
However, consuming a kafka event with a more recent schema version may fail if the schema name has changed.
Also, some schema may have been created with a name that doesn't match your kotlin class name.
Finally, you may want to use the same kotlin class for different schemas.

To solve this, you can use the `@SerialName` and `@AvroAlias` annotations to indicate alternative names for your kotlin classes.
Then, to make accessible the alias'ed types, you need to register those classes in the `Avro` instance used by the serde/serializer/deserializer.

```kotlin
package com.example
@Serializable
enum class TheEnum {
    VALUE1, VALUE2
}
val schemaRegistry = MockSchemaRegistry()
val serializer = ReflectAvro4kKafkaSerializer(isKey = false, schemaRegistry = schemaRegistry)
val bytes = serializer.serialize("topic", TheEnum.VALUE1) // registered schema name is "com.example.TheEnum"

package my.awesome.refactoring.packaging
@Serializable
enum class RefactoredEnum {
    VALUE1, VALUE2
}
val deserializer = ReflectAvro4kKafkaDeserializer(isKey = false, schemaRegistry = schemaRegistry)
deserializer.deserialize("topic", bytes) // throws exception, no class found for schema name "com.example.TheEnum"

@Serializable
@AvroAlias("ServerAppEnum")
enum class MobileAppEnumWithAlias {
    VALUE1, VALUE2
}
val deserializer = ReflectAvro4kKafkaDeserializer(isKey = false, schemaRegistry = schemaRegistry)
deserializer.deserialize("topic", bytes) // still throws exception, the Avro instance doesn't know about MobileAppEnumWithAlias's aliases

val deserializer = ReflectAvro4kKafkaDeserializer(
    avro = Avro {
        // register the class so it can be found by its name or aliases
        contextual(MobileAppEnumWithAlias.serializer())
    },
    isKey = false,
    schemaRegistry = schemaRegistry
)
deserializer.deserialize("topic", bytes) // returns MobileAppEnumWithAlias.VALUE1


```