package com.github.avrokotlin.avro4k.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

public interface AvroEncoder : Encoder {
    @ExperimentalSerializationApi
    public val currentWriterSchema: Schema

    @ExperimentalSerializationApi
    public fun encodeBytes(value: ByteBuffer)

    @ExperimentalSerializationApi
    public fun encodeBytes(value: ByteArray)

    @ExperimentalSerializationApi
    public fun encodeFixed(value: ByteArray)

    @ExperimentalSerializationApi
    public fun encodeFixed(value: GenericFixed)

    /**
     * Helps to encode a value in different ways depending on the type of the writer schema.
     * Each encoder have to return the encoded value for the matched schema.
     *
     * @param kotlinTypeName represents the kotlin type name of the encoded value for debugging purposes as it's used in exceptions. This is not the written avro type name.
     */
    @ExperimentalSerializationApi
    public fun encodeValueResolved(
        vararg encoders: Pair<SchemaTypeMatcher, (Schema) -> Any>,
        kotlinTypeName: String,
    )
}

@ExperimentalSerializationApi
public inline fun <reified T : Any> AvroEncoder.encodeValueResolved(vararg encoders: Pair<SchemaTypeMatcher, (Schema) -> Any>) {
    encodeValueResolved(*encoders, kotlinTypeName = T::class.qualifiedName!!)
}

public sealed class SchemaTypeMatcher {
    public sealed class Scalar : SchemaTypeMatcher() {
        public object BOOLEAN : Scalar()

        public object INT : Scalar()

        public object LONG : Scalar()

        public object FLOAT : Scalar()

        public object DOUBLE : Scalar()

        public object STRING : Scalar()

        public object BYTES : Scalar()

        public object NULL : Scalar()
    }

    public object FirstArray : SchemaTypeMatcher()

    public object FirstMap : SchemaTypeMatcher()

    public sealed class Named : SchemaTypeMatcher() {
        public object FirstFixed : Named()

        public object FirstEnum : Named()

        public data class Fixed(val fullName: String) : Named()

        public data class Enum(val fullName: String) : Named()

        public data class Record(val fullName: String) : Named()
    }

    override fun toString(): String {
        return this::class.simpleName!!
    }
}