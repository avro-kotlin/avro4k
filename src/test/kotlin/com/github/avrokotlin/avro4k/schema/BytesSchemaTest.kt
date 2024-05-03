package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.nullable
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.serializer
import org.apache.avro.Schema

@OptIn(InternalSerializationApi::class)
internal class BytesSchemaTest : FunSpec({

    listOf(
        WrappedByteArray::class,
        ByteArray::class,
        WrappedByteList::class,
        WrappedByteCollection::class,
        WrappedByteSet::class
    ).forEach { type ->
        test("encode $type as BYTES type") {
            AvroAssertions.assertThat(type.serializer())
                .generatesSchema(Schema.create(Schema.Type.BYTES))
        }
        test("encode nullable $type as BYTES type") {
            AvroAssertions.assertThat(type.serializer().nullable)
                .generatesSchema(Schema.create(Schema.Type.BYTES).nullable)
        }
    }
}) {
    @JvmInline
    @Serializable
    private value class WrappedByteArray(val value: ByteArray)

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