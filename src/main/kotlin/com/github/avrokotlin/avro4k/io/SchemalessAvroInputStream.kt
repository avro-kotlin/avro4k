package com.github.avrokotlin.avro4k.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.JsonDecoder
import java.io.EOFException
import java.io.InputStream

/**
 * Abstract implementation of schema-less formats.
 * When using these formats, at least the writer schema must be supplied, as it cannot
 * be loaded from the input source.
 */
abstract class SchemalessAvroInputStream<T>(
    private val input: InputStream,
    private val converter: (Any) -> T,
    writerSchema: Schema,
    readerSchema: Schema,
) : AvroInputStream<T> {
    private val datumReader = GenericDatumReader<GenericRecord>(writerSchema, readerSchema)

    abstract val decoder: Decoder

    override fun close(): Unit = input.close()

    override fun next(): T? {
        val record =
            try {
                datumReader.read(null, decoder)
            } catch (e: EOFException) {
                null
            }
        return when (record) {
            null -> null
            else -> converter(record)
        }
    }
}

class AvroJsonInputStream<T>(
    input: InputStream,
    converter: (Any) -> T,
    writerSchema: Schema,
    readerSchema: Schema,
) :
    SchemalessAvroInputStream<T>(input, converter, writerSchema, readerSchema) {
    override val decoder: JsonDecoder = DecoderFactory.get().jsonDecoder(readerSchema, input)
}

/**
 * An implementation of [AvroInputStream] that reads values of type T
 * written as binary data.
 * See https://avro.apache.org/docs/current/spec.html#binary_encoding
 */
class AvroBinaryInputStream<T>(
    input: InputStream,
    converter: (Any) -> T,
    writerSchema: Schema,
    readerSchema: Schema,
) :
    SchemalessAvroInputStream<T>(input, converter, writerSchema, readerSchema) {
    override val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(input, null)
}