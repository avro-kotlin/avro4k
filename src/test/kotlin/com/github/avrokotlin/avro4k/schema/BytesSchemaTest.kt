package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.internal.nullable
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.serializer
import org.apache.avro.Schema

internal class BytesSchemaTest : FunSpec({
    listOf(
        WrappedObjectByteArray.serializer(),
        serializer<Array<Byte>>(),
        WrappedByteList.serializer(),
        serializer<List<Byte>>(),
        WrappedByteCollection.serializer(),
        serializer<Collection<Byte>>(),
        WrappedByteSet.serializer(),
        serializer<Set<Byte>>()
    ).forEach { serializer ->
        test("encode ${serializer.descriptor} as ARRAY[INT] type instead of BYTES") {
            AvroAssertions.assertThat(serializer)
                .generatesSchema(Schema.createArray(Schema.create(Schema.Type.INT)))
        }
        test("encode nullable ${serializer.descriptor} as ARRAY[INT] type instead of BYTES") {
            AvroAssertions.assertThat(serializer.nullable)
                .generatesSchema(Schema.createArray(Schema.create(Schema.Type.INT)).nullable)
        }
    }

    listOf(
        WrappedByteArray.serializer(),
        serializer<ByteArray>()
    ).forEach { serializer ->
        test("encode ${serializer.descriptor} as BYTES type") {
            AvroAssertions.assertThat(serializer)
                .generatesSchema(Schema.create(Schema.Type.BYTES))
        }
        test("encode nullable ${serializer.descriptor} as BYTES type") {
            AvroAssertions.assertThat(serializer.nullable)
                .generatesSchema(Schema.create(Schema.Type.BYTES).nullable)
        }
    }
}) {
    @JvmInline
    @Serializable
    private value class WrappedByteArray(val value: ByteArray)

    @JvmInline
    @Serializable
    private value class WrappedObjectByteArray(val value: Array<Byte>)

    @JvmInline
    @Serializable
    private value class WrappedByteList(val value: List<Byte>)

    @JvmInline
    @Serializable
    private value class WrappedByteCollection(val value: Collection<Byte>)

    @JvmInline
    @Serializable
    private value class WrappedByteSet(val value: Set<Byte>)
}