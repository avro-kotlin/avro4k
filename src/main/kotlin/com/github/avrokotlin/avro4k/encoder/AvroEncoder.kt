package com.github.avrokotlin.avro4k.encoder

import kotlinx.serialization.SerializationException
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

interface AvroEncoder : Encoder {
    val currentWriterSchema: Schema

    fun encodeBytes(value: ByteBuffer)

    fun encodeBytes(value: ByteArray)

    fun encodeFixed(value: ByteArray)

    fun encodeFixed(value: GenericFixed)

    /**
     * Helps to encode a value in different ways depending on the type of the writer schema.
     * Each encoder have to return the encoded value for the matched schema.
     *
     * @param kotlinTypeName represents the kotlin type name of the encoded value for debugging purposes as it's used in exceptions. This is not the written avro type name.
     */
    fun encodeValueResolved(
        vararg encoders: Pair<SchemaTypeMatcher, (Schema) -> Any>,
        kotlinTypeName: String,
    )
}

inline fun <reified T : Any> AvroEncoder.encodeValueResolved(vararg encoders: Pair<SchemaTypeMatcher, (Schema) -> Any>) {
    encodeValueResolved(*encoders, kotlinTypeName = T::class.qualifiedName!!)
}

sealed class SchemaTypeMatcher {
    sealed class Scalar : SchemaTypeMatcher() {
        object BOOLEAN : Scalar()

        object INT : Scalar()

        object LONG : Scalar()

        object FLOAT : Scalar()

        object DOUBLE : Scalar()

        object STRING : Scalar()

        object BYTES : Scalar()

        object NULL : Scalar()
    }

    object FirstArray : SchemaTypeMatcher()

    object FirstMap : SchemaTypeMatcher()

    sealed class Named : SchemaTypeMatcher() {
        object FirstFixed : Named()

        object FirstEnum : Named()

        data class Fixed(val fullName: String) : Named()

        data class Enum(val fullName: String) : Named()

        data class Record(val fullName: String) : Named()
    }

    override fun toString(): String {
        return this::class.simpleName!!
    }
}

context(Encoder)
internal fun Schema.ensureTypeOf(type: Schema.Type) {
    if (this.type != type) {
        throw SerializationException("Schema $this must be of type $type to be used with ${this@ensureTypeOf::class}")
    }
}