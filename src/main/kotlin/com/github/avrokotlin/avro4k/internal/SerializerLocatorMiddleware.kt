package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.serializer.AvroByteArraySerializer
import com.github.avrokotlin.avro4k.serializer.AvroCollectionSerializer
import com.github.avrokotlin.avro4k.serializer.JsonArrayAvroSerializer
import com.github.avrokotlin.avro4k.serializer.JsonElementAvroSerializer
import com.github.avrokotlin.avro4k.serializer.JsonNullAvroSerializer
import com.github.avrokotlin.avro4k.serializer.JsonObjectAvroSerializer
import com.github.avrokotlin.avro4k.serializer.JsonPrimitiveAvroSerializer
import com.github.avrokotlin.avro4k.serializer.KotlinDurationSerializer
import com.github.avrokotlin.avro4k.serializer.SerialDescriptorWithAvroSchemaDelegate
import com.github.avrokotlin.avro4k.serializer.createSchema
import com.github.avrokotlin.avro4k.serializer.fixed
import com.github.avrokotlin.avro4k.serializer.stringable
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.internal.AbstractCollectionSerializer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.apache.avro.Schema
import kotlin.time.Duration

/**
 * This middleware is here to intercept some native types like kotlin Duration or ByteArray as we want to apply some
 * specific rules on them for generating custom schemas or having specific serialization strategies.
 */
@Suppress("UNCHECKED_CAST")
internal object SerializerLocatorMiddleware {
    fun <T> apply(serializer: SerializationStrategy<T>): SerializationStrategy<T> {
        return when {
            serializer === ByteArraySerializer() -> AvroByteArraySerializer
            serializer === Duration.serializer() -> KotlinDurationSerializer

            serializer === JsonElement.serializer() -> JsonElementAvroSerializer
            serializer === JsonObject.serializer() -> JsonObjectAvroSerializer
            serializer === JsonArray.serializer() -> JsonArrayAvroSerializer
            serializer === JsonPrimitive.serializer() -> JsonPrimitiveAvroSerializer
            serializer === JsonNull.serializer() -> JsonNullAvroSerializer

            else -> serializer
        } as SerializationStrategy<T>
    }

    @OptIn(InternalSerializationApi::class)
    fun <T> apply(deserializer: DeserializationStrategy<T>): DeserializationStrategy<T> {
        return when {
            deserializer === ByteArraySerializer() -> AvroByteArraySerializer
            deserializer === Duration.serializer() -> KotlinDurationSerializer

            deserializer === JsonElement.serializer() -> JsonElementAvroSerializer
            deserializer === JsonObject.serializer() -> JsonObjectAvroSerializer
            deserializer === JsonArray.serializer() -> JsonArrayAvroSerializer
            deserializer === JsonPrimitive.serializer() -> JsonPrimitiveAvroSerializer
            deserializer === JsonNull.serializer() -> JsonNullAvroSerializer

            deserializer is AbstractCollectionSerializer<*, T, *> -> AvroCollectionSerializer(deserializer)
            else -> deserializer
        } as DeserializationStrategy<T>
    }

    fun apply(descriptor: SerialDescriptor): SerialDescriptor {
        return when {
            descriptor === ByteArraySerializer().descriptor -> AvroByteArraySerializer.descriptor
            descriptor === String.serializer().descriptor -> AvroStringSerialDescriptor
            descriptor === Duration.serializer().descriptor -> KotlinDurationSerializer.descriptor
            else -> descriptor
        }
    }
}

private val AvroStringSerialDescriptor: SerialDescriptor =
    SerialDescriptorWithAvroSchemaDelegate(String.serializer().descriptor) { context ->
        context.inlinedElements.firstNotNullOfOrNull {
            it.stringable?.createSchema() ?: it.fixed?.createSchema(it)
        } ?: Schema.create(Schema.Type.STRING)
    }