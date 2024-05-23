package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDefault
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Transient
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.elementNames
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.WeakIdentityHashMap
import java.util.WeakHashMap

internal class RecordResolver(
    private val avro: Avro,
) {
    /**
     * For a class descriptor + writerSchema, it returns a map of the field index to the schema field.
     *
     * Note: We use the descriptor in the key as we could have multiple descriptors for the same record schema, and multiple record schemas for the same descriptor.
     */
    private val fieldCache: MutableMap<SerialDescriptor, MutableMap<Schema, List<ElementDescriptor?>>> = WeakIdentityHashMap()

    /**
     * @return a list of fields for the writer schema, in the same order as the class descriptor. If a field is not found in the schema, the array item is null.
     */
    fun resolveFields(
        writerSchema: Schema,
        classDescriptor: SerialDescriptor,
    ): List<ElementDescriptor?> {
        if (classDescriptor.elementsCount == 0) {
            return emptyList()
        }
        return fieldCache.getOrPut(classDescriptor) { WeakHashMap() }.getOrPut(writerSchema) {
            loadCache(classDescriptor, writerSchema)
        }
    }

    /**
     * Here the different steps to get the schema field corresponding to the serial descriptor element:
     * - class field name -> schema field name
     * - class field name -> schema field aliases
     * - class field aliases -> schema field name
     * - class field aliases -> schema field aliases
     * - if field is optional, returns null
     * - if still not found, [SerializationException] thrown
     */
    private fun loadCache(
        classDescriptor: SerialDescriptor,
        writerSchema: Schema,
    ): List<ElementDescriptor?> {
        val readerSchema = avro.schema(classDescriptor)
        return classDescriptor.elementNames.mapIndexed { elementIndex, _ ->
            val avroFieldName = avro.configuration.fieldNamingStrategy.resolve(classDescriptor, elementIndex)

            val writerField = writerSchema.tryGetField(avroFieldName, classDescriptor, elementIndex)
            val writerFieldSchema = writerField?.schema() ?: readerSchema.getField(avroFieldName).schema()

            // not using the default from reader schema field to simplify the default value parsing
            val readerDefaultAnnotation = classDescriptor.findElementAnnotation<AvroDefault>(elementIndex)

            if (writerField == null && readerDefaultAnnotation == null) {
                if (classDescriptor.isElementOptional(elementIndex)) {
                    // default kotlin values are managed natively by kotlinx.serialization, so we can safely skip the field
                    return@mapIndexed null
                } else {
                    throw SerializationException(
                        "Field '$avroFieldName' at index $elementIndex from descriptor '${classDescriptor.serialName}' not found in schema $writerSchema. " +
                            "Consider removing the field, " +
                            "adding a default value, " +
                            "or annotating it with @${Transient::class.qualifiedName}"
                    )
                }
            } else {
                ElementDescriptor(
                    writerFieldIndex = writerField?.pos(),
                    writerFieldSchema = writerFieldSchema,
                    readerDefaultValue = readerDefaultAnnotation?.parseValue(writerFieldSchema),
                    readerHasDefaultValue = readerDefaultAnnotation != null
                )
            }
        }
    }

    private fun Schema.tryGetField(
        avroFieldName: String,
        classDescriptor: SerialDescriptor,
        elementIndex: Int,
    ): Schema.Field? =
        getField(avroFieldName)
            ?: fields.firstOrNull { avroFieldName in it.aliases() }
            ?: classDescriptor.findElementAnnotation<AvroAlias>(elementIndex)?.value?.let { aliases ->
                fields.firstOrNull { schemaField ->
                    schemaField.name() in aliases || schemaField.aliases().any { it in aliases }
                }
            }
}

internal data class ElementDescriptor(
    val writerFieldIndex: Int?,
    val writerFieldSchema: Schema,
    val readerDefaultValue: Any?,
    val readerHasDefaultValue: Boolean,
)

private fun AvroDefault.parseValue(schema: Schema): Any? {
    if (value.isStartingAsJson()) {
        return Json.parseToJsonElement(value).convertDefaultToObject(schema)
    }
    return JsonPrimitive(value).convertDefaultToObject(schema)
}

private fun JsonElement.convertDefaultToObject(schema: Schema): Any? {
    return when (this) {
        is JsonArray ->
            when (schema.type) {
                Schema.Type.ARRAY -> this.map { it.convertDefaultToObject(schema.elementType) }
                Schema.Type.UNION -> this.convertDefaultToObject(schema.resolveUnion(this, Schema.Type.ARRAY))
                else -> throw SerializationException("Not a valid array value for schema $schema: $this")
            }

        is JsonNull -> null
        is JsonObject ->
            when (schema.type) {
                Schema.Type.RECORD -> {
                    GenericData.Record(schema).apply {
                        entries.forEach { (fieldName, value) ->
                            val schemaField = schema.getField(fieldName)
                            put(schemaField.pos(), value.convertDefaultToObject(schemaField.schema()))
                        }
                    }
                }

                Schema.Type.MAP -> entries.associate { (key, value) -> key to value.convertDefaultToObject(schema.valueType) }
                Schema.Type.UNION -> this.convertDefaultToObject(schema.resolveUnion(this, Schema.Type.RECORD, Schema.Type.MAP))
                else -> throw SerializationException("Not a valid record value for schema $schema: $this")
            }

        is JsonPrimitive ->
            when (schema.type) {
                Schema.Type.BYTES -> this.content.toByteArray()
                Schema.Type.FIXED -> GenericData.Fixed(schema, this.content.toByteArray())
                Schema.Type.STRING -> this.content
                Schema.Type.BOOLEAN -> this.boolean

                Schema.Type.INT,
                Schema.Type.LONG,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                -> this.content.toBigDecimal()

                Schema.Type.UNION ->
                    this.convertDefaultToObject(
                        schema.resolveUnion(
                            this,
                            Schema.Type.BYTES,
                            Schema.Type.FIXED,
                            Schema.Type.STRING,
                            Schema.Type.BOOLEAN,
                            Schema.Type.INT,
                            Schema.Type.LONG,
                            Schema.Type.FLOAT,
                            Schema.Type.DOUBLE
                        )
                    )

                else -> throw SerializationException("Not a valid primitive value for schema $schema: $this")
            }
    }
}

private fun Schema.resolveUnion(
    value: JsonElement?,
    vararg expectedTypes: Schema.Type,
): Schema {
    val index = types.indexOfFirst { it.type in expectedTypes }
    if (index < 0) {
        throw SerializationException("Union type does not contain one of ${expectedTypes.asList()}, unable to convert default value: $value")
    }
    return types[index]
}