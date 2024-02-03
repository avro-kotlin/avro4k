package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AnnotationExtractor
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroInternalConfiguration
import com.github.avrokotlin.avro4k.AvroJsonProp
import com.github.avrokotlin.avro4k.AvroNamespaceOverride
import com.github.avrokotlin.avro4k.AvroProp
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.JsonProperties
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

@ExperimentalSerializationApi
class ClassSchemaFor(
    private val descriptor: SerialDescriptor,
    private val configuration: AvroInternalConfiguration,
    private val serializersModule: SerializersModule,
    private val resolvedSchemas: MutableMap<RecordName, Schema>,
) : SchemaFor {
    private val entityAnnotations = AnnotationExtractor(descriptor.annotations)
    private val naming = configuration.recordNamingStrategy.resolve(descriptor, descriptor.serialName)
    private val json by lazy {
        Json {
            serializersModule = this@ClassSchemaFor.serializersModule
        }
    }

    override fun schema(): Schema =
        if (descriptor.isInline) {
            buildField(0).schema()
        } else {
            dataClassSchema()
        }

    private fun dataClassSchema(): Schema {
        // return schema if already resolved - recursive circuit breaker
        resolvedSchemas[naming]?.let { return it }

        // create new schema without fields
        val record = Schema.createRecord(naming.name, entityAnnotations.doc(), naming.namespace, false)

        // add schema without fields right now, so that fields could recursively use it
        resolvedSchemas[naming] = record

        val fields =
            (0 until descriptor.elementsCount)
                .map { index -> buildField(index) }

        record.fields = fields
        entityAnnotations.aliases().forEach { record.addAlias(it) }
        entityAnnotations.props().forEach { (k, v) -> record.addProp(k, v) }
        entityAnnotations.jsonProps().forEach { (k, v) -> record.addProp(k, json.parseToJsonElement(v).convertToAvroDefault()) }

        return record
    }

    private fun buildField(index: Int): Schema.Field {
        val fieldTypeDescriptor = descriptor.getElementDescriptor(index)
        val annos = AnnotationExtractor(descriptor.getElementAnnotations(index))
        val fieldSpecificNamespace: String? = descriptor.getElementAnnotations(index).filterIsInstance<AvroNamespaceOverride>().firstOrNull()?.value
        val fieldName = configuration.fieldNamingStrategy.resolve(descriptor, index, descriptor.getElementName(index))
        val schema =
            getFixedSchema(fieldName, annos) ?: schemaFor(
                serializersModule,
                fieldTypeDescriptor,
                descriptor.getElementAnnotations(index),
                configuration,
                resolvedSchemas
            ).schema()

        // If the field is annotated with a specific namespace, then we need to override the namespace of the field's schema
        val schemaWithResolvedNamespace = fieldSpecificNamespace?.let { schema.overrideNamespace(it) } ?: schema

        val default: Any? = getDefaultValue(annos, schemaWithResolvedNamespace, fieldTypeDescriptor)

        val field = Schema.Field(fieldName, schemaWithResolvedNamespace, annos.doc(), default)
        field.mutateFieldFromAnnotations(this.descriptor.getElementAnnotations(index))
        return field
    }

    private fun getFixedSchema(
        fieldName: String,
        annos: AnnotationExtractor,
    ): Schema? {
        val size = annos.fixed() ?: return null
        return SchemaBuilder.fixed(fieldName)
            .doc(annos.doc())
            .namespace(naming.namespace)
            .size(size)
    }

    private fun Schema.Field.mutateFieldFromAnnotations(annotations: List<Annotation>) =
        annotations.forEach {
            when (it) {
                is AvroProp -> this.addProp(it.key, it.value)
                is AvroJsonProp -> this.addProp(it.key, json.parseToJsonElement(it.jsonValue).convertToAvroDefault())
                is AvroAlias -> it.value.forEach { this.addAlias(it) }
            }
        }

    private fun getDefaultValue(
        annos: AnnotationExtractor,
        schemaWithResolvedNamespace: Schema,
        fieldTypeDescriptor: SerialDescriptor,
    ) = annos.default()?.let { annotationDefaultValue ->
        when {
            annotationDefaultValue == Avro.NULL -> Schema.Field.NULL_DEFAULT_VALUE
            schemaWithResolvedNamespace.extractNonNull().type in
                listOf(
                    Schema.Type.FIXED,
                    Schema.Type.BYTES,
                    Schema.Type.STRING,
                    Schema.Type.ENUM
                )
            -> annotationDefaultValue

            else -> json.parseToJsonElement(annotationDefaultValue).convertToAvroDefault()
        }
    } ?: if (configuration.implicitNulls && fieldTypeDescriptor.isNullable) {
        Schema.Field.NULL_DEFAULT_VALUE
    } else {
        null
    }

    private fun JsonElement.convertToAvroDefault(): Any {
        return when (this) {
            is JsonNull -> JsonProperties.NULL_VALUE
            is JsonObject -> this.map { Pair(it.key, it.value.convertToAvroDefault()) }.toMap()
            is JsonArray -> this.map { it.convertToAvroDefault() }.toList()
            is JsonPrimitive ->
                when {
                    this.isString -> this.content
                    this.booleanOrNull != null -> this.boolean
                    else -> {
                        val number = this.content.toBigDecimal()
                        if (number.scale() <= 0) {
                            number.toBigInteger()
                        } else {
                            number
                        }
                    }
                }
        }
    }
}