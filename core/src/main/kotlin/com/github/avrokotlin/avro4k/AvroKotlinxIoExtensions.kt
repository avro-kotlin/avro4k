package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decodeWithApacheDecoder
import com.github.avrokotlin.avro4k.internal.decoder.direct.KotlinxIoDecoder
import com.github.avrokotlin.avro4k.internal.encodeWithApacheEncoder
import com.github.avrokotlin.avro4k.internal.encoder.direct.KotlinxIoEncoder
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import org.apache.avro.Schema

/**
 * Encode the given [value] as binary avro into the given [sink], respecting the [writerSchema].
 *
 * For any custom encoding or conversion of the [value] or its inner type, you may need to register a custom [kotlinx.serialization.KSerializer] in [Avro.serializersModule].
 *
 * Note that as the [sink] is buffered, you may need to call [Sink.flush] to ensure all data is written out.
 * This method does not close the [sink], so you need to handle it to avoid resource leaks.
 *
 * @param writerSchema the schema to use for encoding the value which must be compatible with the given [serializer].
 * @param serializer the serialization strategy to use for encoding the value. You may prefer the other extension methods without the serializer parameter for convenience.
 * @param value the value to encode.
 * @param sink the sink to write the encoded value to.
 *
 * @see Avro.encodeToByteArray
 * @see kotlinx.io.asSink
 * @see com.github.avrokotlin.avro4k.serializer.AvroSerializer
 */
public fun <T> Avro.encodeToSink(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
    sink: Sink,
) {
    encodeWithApacheEncoder(writerSchema, serializer, value, KotlinxIoEncoder(sink))
}

/**
 * Encode the given [value] as binary avro into the given [sink], using the [value]'s schema as the writer schema.
 *
 * For any custom schema, encoding or conversion of the [value] or its inner type, you may need to register a custom [kotlinx.serialization.KSerializer] in [Avro.serializersModule].
 *
 * Note that as the [sink] is buffered, you may need to call [Sink.flush] to ensure all data is written out.
 * This method does not close the [sink], so you need to handle it to avoid resource leaks.
 *
 * @param value the value to encode.
 * @param sink the sink to write the encoded value to.
 *
 * @see Avro.encodeToByteArray
 * @see Avro.schema
 * @see kotlinx.io.asSink
 * @see kotlinx.serialization.modules.SerializersModule.serializer
 * @see com.github.avrokotlin.avro4k.serializer.AvroSerializer
 */
public inline fun <reified T> Avro.encodeToSink(
    value: T,
    sink: Sink,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToSink(schema(serializer), serializer, value, sink)
}

/**
 * Encode the given [value] as binary avro into the given [sink], respecting the [writerSchema].
 *
 * For any custom encoding or conversion of the [value] or its inner type, you may need to register a custom [kotlinx.serialization.KSerializer] in [Avro.serializersModule].
 *
 * Note that as the [sink] is buffered, you may need to call [Sink.flush] to ensure all data is written out.
 * This method does not close the [sink], so you need to handle it to avoid resource leaks.
 *
 * @param writerSchema the schema to use for encoding the value which must be compatible with the [value].
 * @param value the value to encode.
 * @param sink the sink to write the encoded value to.
 *
 * @see Avro.encodeToByteArray
 * @see kotlinx.io.asSink
 * @see kotlinx.serialization.modules.SerializersModule.serializer
 * @see com.github.avrokotlin.avro4k.serializer.AvroSerializer
 */
public inline fun <reified T> Avro.encodeToSink(
    writerSchema: Schema,
    value: T,
    sink: Sink,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToSink(writerSchema, serializer, value, sink)
}

/**
 * Decode the value from a [source] assuming the data is respecting the given [writerSchema].
 * If during the decoding, the written data does not match the [writerSchema], you may end up to unexpected results or an exception.
 *
 * For any custom encoding or conversion of type [T] or its inner type, you may need to register a custom [kotlinx.serialization.KSerializer] in [Avro.serializersModule].
 *
 * @param writerSchema the schema to use for decoding the value.
 * @param deserializer the deserialization strategy to use for decoding the value. You may prefer the other extension methods without the deserializer parameter for convenience.
 * @param source the source to read the encoded value from.
 * @return the decoded value of type [T].
 *
 * @see Avro.decodeFromByteArray
 * @see kotlinx.io.asSink
 * @see com.github.avrokotlin.avro4k.serializer.AvroSerializer
 */
public fun <T> Avro.decodeFromSource(
    writerSchema: Schema,
    deserializer: DeserializationStrategy<T>,
    source: Source,
): T {
    return decodeWithApacheDecoder(
        writerSchema,
        deserializer,
        KotlinxIoDecoder(source)
    )
}

/**
 * Decode the value from a [source] assuming the data is respecting the schema inferred from [T] using [Avro.schema].
 * If during the decoding, the written data does not match [T]'s schema, you may end up to unexpected results or an exception.
 *
 * For any custom encoding or conversion of type [T] or its inner type, you may need to register a custom [kotlinx.serialization.KSerializer] in [Avro.serializersModule].
 *
 * @param source the source to read the encoded value from.
 * @return the decoded value of type [T].
 *
 * @see Avro.decodeFromByteArray
 * @see Avro.schema
 * @see kotlinx.serialization.modules.SerializersModule.serializer
 * @see kotlinx.io.asSink
 * @see com.github.avrokotlin.avro4k.serializer.AvroSerializer
 */
public inline fun <reified T> Avro.decodeFromSource(source: Source): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromSource(schema(serializer.descriptor), serializer, source)
}

/**
 * Decode the value from a [source] assuming the data is respecting the given [writerSchema].
 * If during the decoding, the written data does not match the [writerSchema], you may end up to unexpected results or an exception.
 *
 * For any custom encoding or conversion of type [T] or its inner type, you may need to register a custom [kotlinx.serialization.KSerializer] in [Avro.serializersModule].
 *
 * @param writerSchema the schema to use for decoding the value.
 * @param source the source to read the encoded value from.
 * @return the decoded value of type [T].
 *
 * @see Avro.decodeFromByteArray
 * @see kotlinx.serialization.modules.SerializersModule.serializer
 * @see kotlinx.io.asSink
 * @see com.github.avrokotlin.avro4k.serializer.AvroSerializer
 */
public inline fun <reified T> Avro.decodeFromSource(
    writerSchema: Schema,
    source: Source,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromSource(writerSchema, serializer, source)
}