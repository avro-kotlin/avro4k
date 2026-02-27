package com.github.avrokotlin.avro4k.internal.decoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.io.Decoder

internal class RecordAsMapDirectDecoder(
    private val writerRecordSchema: Schema,
    avro: Avro,
    binaryDecoder: Decoder,
) : AbstractAvroDirectDecoder(avro, binaryDecoder) {
    override lateinit var currentWriterSchema: Schema
    private var fieldNameToRead: String? = null

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int {
        return writerRecordSchema.fields.size
    }

    override fun beginElement(
        descriptor: SerialDescriptor,
        index: Int,
    ) {
        val field = writerRecordSchema.fields[index / 2]
        if (index % 2 == 0) {
            currentWriterSchema = KEY_SCHEMA
            fieldNameToRead = field.name()
        } else {
            currentWriterSchema = field.schema()
            fieldNameToRead = null
        }
    }

    override fun decodeString(): String {
        return fieldNameToRead ?: super.decodeString()
    }

    @OptIn(ExperimentalSerializationApi::class)
    override fun decodeSequentially() = true

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }

    companion object {
        private val KEY_SCHEMA = Schema.create(Schema.Type.STRING)
    }

    override fun decodeBoolean(): Boolean {
        ensureNotFieldNameDecoding()
        return super.decodeBoolean()
    }

    override fun decodeByte(): Byte {
        ensureNotFieldNameDecoding()
        return super.decodeByte()
    }

    override fun decodeBytes(): ByteArray {
        ensureNotFieldNameDecoding()
        return super.decodeBytes()
    }

    override fun decodeChar(): Char {
        ensureNotFieldNameDecoding()
        return super.decodeChar()
    }

    override fun decodeFixed(): GenericFixed {
        ensureNotFieldNameDecoding()
        return super.decodeFixed()
    }

    override fun decodeShort(): Short {
        ensureNotFieldNameDecoding()
        return super.decodeShort()
    }

    override fun decodeInt(): Int {
        ensureNotFieldNameDecoding()
        return super.decodeInt()
    }

    override fun decodeLong(): Long {
        ensureNotFieldNameDecoding()
        return super.decodeLong()
    }

    override fun decodeFloat(): Float {
        ensureNotFieldNameDecoding()
        return super.decodeFloat()
    }

    override fun decodeNull(): Nothing? {
        ensureNotFieldNameDecoding()
        return super.decodeNull()
    }

    override fun decodeDouble(): Double {
        ensureNotFieldNameDecoding()
        return super.decodeDouble()
    }

    private fun ensureNotFieldNameDecoding() {
        if (fieldNameToRead != null) {
            throw UnsupportedOperationException("Misused decoding: expected to decode the field name")
        }
    }
}