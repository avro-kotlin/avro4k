package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.AvroSchema
import com.github.avrokotlin.avro4k.AvroSchema.ArraySchema
import com.github.avrokotlin.avro4k.AvroSchema.BooleanSchema
import com.github.avrokotlin.avro4k.AvroSchema.BytesSchema
import com.github.avrokotlin.avro4k.AvroSchema.DoubleSchema
import com.github.avrokotlin.avro4k.AvroSchema.EnumSchema
import com.github.avrokotlin.avro4k.AvroSchema.FixedSchema
import com.github.avrokotlin.avro4k.AvroSchema.FloatSchema
import com.github.avrokotlin.avro4k.AvroSchema.IntSchema
import com.github.avrokotlin.avro4k.AvroSchema.LongSchema
import com.github.avrokotlin.avro4k.AvroSchema.MapSchema
import com.github.avrokotlin.avro4k.AvroSchema.NamedSchema
import com.github.avrokotlin.avro4k.AvroSchema.NullSchema
import com.github.avrokotlin.avro4k.AvroSchema.RecordSchema
import com.github.avrokotlin.avro4k.AvroSchema.StringSchema
import com.github.avrokotlin.avro4k.AvroSchema.UnionSchema
import com.github.avrokotlin.avro4k.internal.jsonElement
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.ElementLocation
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.json.JsonElement

internal class InlineClassVisitor(
    private val context: VisitorContext,
    private val onSchemaBuilt: (AvroSchema) -> Unit,
) : SerialDescriptorInlineClassVisitor {
    override fun visitInlineClassElement(
        inlineClassDescriptor: SerialDescriptor,
        inlineElementIndex: Int,
    ): SerialDescriptorValueVisitor {
        val inlinedElements = context.inlinedElements + ElementLocation(inlineClassDescriptor, inlineElementIndex)
        return ValueVisitor(context.copy(inlinedElements = inlinedElements)) { generatedSchema ->
            val annotations = InlineClassFieldAnnotations(inlineClassDescriptor)

            val props = annotations.props.toList()
            val schema =
                if (props.isNotEmpty()) {
                    if (generatedSchema is NamedSchema) {
                        throw SerializationException(
                            "The value class property '${inlineClassDescriptor.serialName}.${inlineClassDescriptor.getElementName(0)}' has " +
                                    "forbidden additional properties $props for the named schema ${generatedSchema.fullName}. " +
                                    "Please create your own serializer extending ${AvroSerializer::class.qualifiedName} to add properties to a named schema."
                        )
                    }
                    generatedSchema.withAdditionalProperties(props.associate { it.key to it.jsonElement })
                } else {
                    generatedSchema
                }

            onSchemaBuilt(schema)
        }
    }
}

private fun AvroSchema.withAdditionalProperties(additionalProps: Map<String, JsonElement>): AvroSchema {
    return when (this) {
        is ArraySchema -> copy(props = props + additionalProps)
        is MapSchema -> copy(props = props + additionalProps)
        is EnumSchema -> copy(props = props + additionalProps)
        is FixedSchema -> copy(props = props + additionalProps)
        is RecordSchema -> copy(props = props + additionalProps)
        is NullSchema -> copy(props = props + additionalProps)
        is BooleanSchema -> copy(props = props + additionalProps)
        is BytesSchema -> copy(props = props + additionalProps)
        is DoubleSchema -> copy(props = props + additionalProps)
        is FloatSchema -> copy(props = props + additionalProps)
        is IntSchema -> copy(props = props + additionalProps)
        is LongSchema -> copy(props = props + additionalProps)
        is StringSchema -> copy(props = props + additionalProps)
        is UnionSchema -> throw IllegalStateException("UnionSchema does not have properties")
    }
}