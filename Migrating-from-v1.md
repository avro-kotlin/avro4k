Here is the guide of how to migrate from Avro4k v1 to v2 using examples.

> [!INFO]
> If you are missing a migration need, please [file an issue](https://github.com/avro-kotlin/avro4k/issues/new/choose) or [make a PR](https://github.com/avro-kotlin/avro4k/compare).

## Pure avro serialization

```kotlin
// Previously
val bytes = Avro.default.encodeToByteArray(TheDataClass.serializer(), TheDataClass(...))
Avro.default.decodeFromByteArray(TheDataClass.serializer(), bytes)

// Now
val bytes = Avro.encodeToByteArray(TheDataClass(...))
Avro.decodeFromByteArray<TheDataClass>(bytes)
```

## Set a field default value to null

```kotlin
// Previously
data class TheDataClass(
    @AvroDefault(Avro.NULL)
    val field: String?
)

// Now
// ... Nothing, as it is the default behavior!
data class TheDataClass(
    val field: String?
)
```

## generic data serialization
Convert a kotlin data class to a `GenericRecord` to then be handled by a `GenericDatumWriter` in avro.

```kotlin
// Previously
val genericRecord: GenericRecord = Avro.default.toRecord(TheDataClass.serializer(), TheDataClass(...))
Avro.default.fromRecord(TheDataClass.serializer(), genericRecord)

// Now
val genericData: Any? = Avro.encodeToGenericData(TheDataClass(...))
Avro.decodeFromGenericData<TheDataClass>(genericData)
```


## Configure the `Avro` instance

```kotlin
// Previously
val avro = Avro(
    AvroConfiguration(
        namingStrategy = FieldNamingStrategy.SnackCase,
        implicitNulls = true,
    ),
    SerializersModule {
         contextual(CustomSerializer())
    }
)

// Now
val avro = Avro {
    namingStrategy = FieldNamingStrategy.SnackCase
    implicitNulls = true
    serializersModule = SerializersModule {
         contextual(CustomSerializer())
    }
}
```

## Changing the name of a record

```kotlin
// Previously
@AvroName("TheName")
@AvroNamespace("a.custom.namespace")
data class TheDataClass(...)

// Now
@SerialName("a.custom.namespace.TheName")
data class TheDataClass(...)
```

## Writing an avro object container file with a custom field naming strategy

```kotlin
// Previously
Files.newOutputStream(Path("/your/file.avro")).use { outputStream ->
    Avro(AvroConfiguration(namingStrategy = SnakeCaseNamingStrategy))
        .openOutputStream(TheDataClass.serializer()) { encodeFormat = AvroEncodeFormat.Data(CodecFactory.snappyCodec()) }
        .to(outputStream)
        .write(TheDataClass(...))
        .write(TheDataClass(...))
        .write(TheDataClass(...))
        .close()
}


// Now
val dataSequence = sequenceOf(
    TheDataClass(...),
    TheDataClass(...),
    TheDataClass(...),
)
val avro = Avro { fieldNamingStrategy = FieldNamingStrategy.SnakeCase }
Files.newOutputStream(Path("/your/file.avro")).use { outputStream ->
    AvroObjectContainerFile(avro)
        .encodeToStream(dataSequence, outputStream) {
            codec(CodecFactory.snappyCodec())
            // you can also add your metadata !
            metadata("myProp", 1234L)
            metadata("a string metadata", "hello")
        }
}
```

