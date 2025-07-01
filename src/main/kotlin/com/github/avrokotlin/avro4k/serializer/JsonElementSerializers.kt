package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.UnionDecoder
import com.github.avrokotlin.avro4k.internal.descriptor
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.internal.AbstractCollectionSerializer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.double
import kotlinx.serialization.json.float
import kotlinx.serialization.json.int
import kotlinx.serialization.json.long
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.reflect.KClass

@Suppress("UNCHECKED_CAST")
internal val KotlinxJsonSerializersModule: SerializersModule =
    SerializersModule {
        // When serializing JsonPrimitive("string"), JsonLiteral is instantiated under the hood.
        // So we need to register it to allow using Avro.encodeToByteArray(schema, JsonPrimitive("string")).
        // We use a runCatching to ensure that a new version of kotlinx.serialization doesn't break this.
        runCatching { Class.forName("kotlinx.serialization.json.JsonLiteral").kotlin }
            .onSuccess { contextual(it as KClass<Any>, JsonPrimitiveAvroSerializer as KSerializer<Any>) }
    }

internal object JsonElementAvroSerializer : AvroSerializer<JsonElement>(JsonElement::class.simpleName!!) {
    override fun serializeAvro(
        encoder: AvroEncoder,
        value: JsonElement,
    ) {
        when (value) {
            is JsonPrimitive -> JsonPrimitiveAvroSerializer.serializeAvro(encoder, value)
            is JsonArray -> JsonArrayAvroSerializer.serializeAvro(encoder, value)
            is JsonObject -> JsonObjectAvroSerializer.serializeAvro(encoder, value)
        }
    }

    override fun deserializeAvro(decoder: AvroDecoder): JsonElement {
        (decoder as UnionDecoder).decodeAndResolveUnion()

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        return when (decoder.currentWriterSchema.type) {
            Schema.Type.DOUBLE,
            Schema.Type.BOOLEAN,
            Schema.Type.INT,
            Schema.Type.LONG,
            Schema.Type.FLOAT,
            Schema.Type.STRING,
            Schema.Type.ENUM,
            Schema.Type.BYTES,
            Schema.Type.FIXED,
            Schema.Type.NULL,
            -> JsonPrimitiveAvroSerializer.deserializeAvro(decoder)
            Schema.Type.ARRAY -> JsonArrayAvroSerializer.deserializeAvro(decoder)
            Schema.Type.MAP, Schema.Type.RECORD -> JsonObjectAvroSerializer.deserializeAvro(decoder)
            Schema.Type.UNION -> throw UnsupportedOperationException("Union should be already resolved")
        }
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException("Not possible to generate schema from generic data as it is only known at runtime")
    }
}

internal object JsonObjectAvroSerializer : AvroSerializer<JsonObject>(JsonObject::class.simpleName!!) {
    @OptIn(InternalSerializationApi::class)
    private val mapSerializer =
        AvroCollectionSerializer(MapSerializer(String.serializer(), JsonElementAvroSerializer) as AbstractCollectionSerializer<*, Map<String, JsonElement>, *>)

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: JsonObject,
    ) {
        encodeUnionIndex(encoder) {
            val index = encoder.currentWriterSchema.types.indexOfFirst { it.type == Schema.Type.MAP || it.type == Schema.Type.RECORD }
            if (index == -1) {
                throw UnsupportedOperationException("Could not find MAP or RECORD type in union ${encoder.currentWriterSchema} to serialize JsonObject $value")
            }
            index
        }
        // TODO maybe we should handle records as maps natively
        when (encoder.currentWriterSchema.type) {
            Schema.Type.RECORD -> encodeRecord(encoder, value)
            Schema.Type.MAP -> mapSerializer.serialize(encoder, value)
            else -> throw SerializationException("Unsupported schema type ${encoder.currentWriterSchema.type}")
        }
    }

    override fun deserializeAvro(decoder: AvroDecoder): JsonObject {
        (decoder as UnionDecoder).decodeAndResolveUnion()
        // TODO maybe we should handle records as maps natively
        return JsonObject(
            when (decoder.currentWriterSchema.type) {
                Schema.Type.RECORD -> decodeRecord(decoder.currentWriterSchema, decoder)
                Schema.Type.MAP -> mapSerializer.deserialize(decoder)
                else -> throw SerializationException("Unsupported schema type ${decoder.currentWriterSchema.type}")
            }
        )
    }

    private fun encodeRecord(encoder: AvroEncoder, value: JsonObject) {
        val descriptor = encoder.currentWriterSchema.descriptor()
        encoder.encodeStructure(descriptor) {
            value.forEach { (itemKey, itemValue) ->
                encodeSerializableElement(descriptor, descriptor.getElementIndex(itemKey), JsonElementAvroSerializer, itemValue)
            }
        }
    }

    private fun decodeRecord(
        schema: Schema,
        decoder: AvroDecoder,
    ): Map<String, JsonElement> {
        val serialDescriptor = schema.descriptor()
        val outputMap = mutableMapOf<String, JsonElement>()
        decoder.decodeStructure(serialDescriptor) {
            do {
                when (val index = decodeElementIndex(serialDescriptor)) {
                    CompositeDecoder.DECODE_DONE -> break
                    else -> outputMap[schema.fields[index].name()] = decodeNullableSerializableElement(serialDescriptor, index, JsonElementAvroSerializer) ?: JsonNull
                }
            } while (true)
        }
        return outputMap
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException(
            "Not possible to generate schema from ${this::class.qualifiedName} as its schema is related to the serialized data itself. " +
                "Do not use ${Avro::class.qualifiedName}#${Avro::schema.name}() with this type."
        )
    }
}

internal object JsonArrayAvroSerializer : AvroSerializer<JsonArray>(JsonArray::class.simpleName!!) {
    @OptIn(InternalSerializationApi::class)
    private val listSerializer = AvroCollectionSerializer(ListSerializer(JsonElementAvroSerializer) as AbstractCollectionSerializer<*, List<JsonElement>, *>)

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: JsonArray,
    ) {
        encodeUnionIndex(encoder) {
            val index = encoder.currentWriterSchema.types.indexOfFirst { it.type == Schema.Type.ARRAY }
            if (index == -1) {
                throw UnsupportedOperationException("Could not find ARRAY type in union ${encoder.currentWriterSchema} to serialize ${JsonArray::class.qualifiedName} $value")
            }
            index
        }
        serializeGeneric(encoder, value)
    }

    override fun deserializeAvro(decoder: AvroDecoder): JsonArray {
        (decoder as UnionDecoder).decodeAndResolveUnion()
        return deserializeGeneric(decoder)
    }

    override fun serializeGeneric(encoder: Encoder, value: JsonArray) {
        listSerializer.serialize(encoder, value)
    }

    override fun deserializeGeneric(decoder: Decoder): JsonArray {
        return JsonArray(listSerializer.deserialize(decoder))
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException(
            "Not possible to generate schema from ${this::class.qualifiedName} as its schema is related to the serialized data itself. " +
                "Do not use ${Avro::class.qualifiedName}#${Avro::schema.name}() with this type."
        )
    }
}

internal object JsonPrimitiveAvroSerializer : AvroSerializer<JsonPrimitive>(JsonPrimitive::class.simpleName!!) {
    override fun serializeAvro(encoder: AvroEncoder, value: JsonPrimitive) {
        if (value === JsonNull) {
            JsonNullAvroSerializer.serializeAvro(encoder, value)
            return
        }
        encodeUnionIndex(encoder) {
            throw NotImplementedError("Primitive json types are not supported yet when serialized using complex union schema")
        }

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (encoder.currentWriterSchema.type) {
            Schema.Type.DOUBLE -> encoder.encodeDouble(value.double)
            Schema.Type.BOOLEAN -> encoder.encodeBoolean(value.boolean)
            Schema.Type.INT -> encoder.encodeInt(value.int)
            Schema.Type.LONG -> encoder.encodeLong(value.long)
            Schema.Type.FLOAT -> encoder.encodeFloat(value.float)
            Schema.Type.STRING, Schema.Type.ENUM -> encoder.encodeString(value.content)
            Schema.Type.BYTES, Schema.Type.FIXED -> encoder.encodeBase64AsBytes(value.content)

            Schema.Type.UNION -> throw UnsupportedOperationException("union should be already resolved")

            Schema.Type.ARRAY,
            Schema.Type.RECORD,
            Schema.Type.MAP,
            Schema.Type.NULL,
            -> throw UnsupportedOperationException(
                "Unsupported avro type ${encoder.currentWriterSchema.type} to serialize ${value::class.qualifiedName} '$value' using avro schema ${encoder.currentWriterSchema}"
            )
        }
    }

    override fun deserializeAvro(decoder: AvroDecoder): JsonPrimitive {
        (decoder as UnionDecoder).decodeAndResolveUnion()

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        return when (decoder.currentWriterSchema.type) {
            Schema.Type.DOUBLE -> JsonPrimitive(decoder.decodeDouble())
            Schema.Type.BOOLEAN -> JsonPrimitive(decoder.decodeBoolean())
            Schema.Type.INT -> JsonPrimitive(decoder.decodeInt())
            Schema.Type.LONG -> JsonPrimitive(decoder.decodeLong())
            Schema.Type.FLOAT -> JsonPrimitive(decoder.decodeFloat())
            Schema.Type.NULL -> decoder.decodeNull().let { JsonNull }
            Schema.Type.STRING, Schema.Type.ENUM -> JsonPrimitive(decoder.decodeString())
            Schema.Type.BYTES, Schema.Type.FIXED -> JsonPrimitive(decoder.decodeBytesAsBase64())

            Schema.Type.UNION -> throw UnsupportedOperationException("union should be already resolved")

            Schema.Type.ARRAY,
            Schema.Type.RECORD,
            Schema.Type.MAP,
            -> throw UnsupportedOperationException(
                "Unsupported schema type ${decoder.currentWriterSchema.type} to deserialize ${JsonPrimitive::class.qualifiedName} using schema ${decoder.currentWriterSchema}"
            )
        }
    }

    @OptIn(ExperimentalEncodingApi::class)
    private fun AvroDecoder.decodeBytesAsBase64(): String {
        return Base64.Mime.encode(decodeBytes())
    }

    @OptIn(ExperimentalEncodingApi::class)
    private fun AvroEncoder.encodeBase64AsBytes(base64: String) {
        encodeBytes(Base64.Mime.decode(base64))
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException(
            "Not possible to generate schema from ${this::class.qualifiedName} as its schema is related to the serialized data itself. " +
                "Do not use ${Avro::class.qualifiedName}#${Avro::schema.name}() with this type."
        )
    }
}

internal object JsonNullAvroSerializer : AvroSerializer<JsonNull>(JsonNull::class.simpleName!!) {
    override fun serializeAvro(encoder: AvroEncoder, value: JsonNull) {
        if (encoder.currentWriterSchema.isUnion) {
            val index = encoder.currentWriterSchema.types.indexOfFirst { it.type == Schema.Type.NULL }
            if (index == -1) {
                throw UnsupportedOperationException("Could not find NULL type in union ${encoder.currentWriterSchema} to serialize JsonNull")
            }
            encoder.encodeUnionIndex(index)
        } else if (encoder.currentWriterSchema.type != Schema.Type.NULL) {
            throw UnsupportedOperationException("Encoding JsonNull to non-null schema is forbidden")
        }
        encoder.encodeNull()
    }

    override fun deserializeAvro(decoder: AvroDecoder): JsonNull {
        (decoder as UnionDecoder).decodeAndResolveUnion()

        if (decoder.currentWriterSchema.type != Schema.Type.NULL) {
            throw SerializationException("Expected NULL schema type but got ${decoder.currentWriterSchema.type}")
        }

        decoder.decodeNull()
        return JsonNull
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException(
            "Not possible to generate schema from ${this::class.qualifiedName} as its schema is related to the serialized data itself. " +
                "Do not use ${Avro::class.qualifiedName}#${Avro::schema.name}() with this type."
        )
    }
}

private inline fun encodeUnionIndex(encoder: AvroEncoder, complexIndexResolver: () -> Int) {
    val schema = encoder.currentWriterSchema
    if (schema.isUnion) {
        val index: Int =
            if (schema.types.size == 2) {
                // common use case for nullable types like [<type>, NULL] or [NULL, <type>]
                val nullIndex = schema.types.indexOfFirst { it.type == Schema.Type.NULL }
                when (nullIndex) {
                    0 -> 1
                    1 -> 0
                    else -> complexIndexResolver()
                }
            } else {
                complexIndexResolver()
            }
        encoder.encodeUnionIndex(index)
    }
}