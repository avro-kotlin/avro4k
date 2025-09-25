package com.github.avrokotlin.avro4k

import org.apache.avro.JsonProperties
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificData

internal sealed interface WithProps {
    val props: Map<String, Any?>
}

internal sealed interface WithDoc {
    val doc: String?
}

internal sealed interface WithAliases {
    val aliases: Set<String>
}

internal sealed interface ByteArraySchema

private const val MAX_ARRAY_VM_LIMIT = Int.MAX_VALUE - 8

internal val TypeSafeSchema.actualJavaClassName: String?
    get() = props[SpecificData.CLASS_PROP] as? String

internal val TypeSafeSchema.logicalTypeName: String?
    get() = props["logicalType"] as? String

internal val TypeSafeSchema.CollectionSchema.ArraySchema.actualElementClass: String?
    get() = props[SpecificData.ELEMENT_PROP] as? String

internal val TypeSafeSchema.CollectionSchema.MapSchema.actualKeyClass: String?
    get() = props[SpecificData.KEY_CLASS_PROP] as? String

internal sealed interface TypeSafeSchema : WithProps {
    val originalSchema: Schema

    val type: SchemaType
    val isNullable: Boolean

    val name: String
    val fullName: String
        get() = name

    enum class SchemaType {
        BOOLEAN,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BYTES,
        STRING,

        RECORD,
        ENUM,
        FIXED,

        ARRAY,
        MAP,

        UNION,
    }

    sealed interface PrimitiveSchema : TypeSafeSchema {
        data class BooleanSchema(
            override val originalSchema: Schema,
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : PrimitiveSchema {
            override val type: SchemaType
                get() = SchemaType.BOOLEAN
            override val name: String
                get() = "boolean"
        }

        data class IntSchema(
            override val originalSchema: Schema,
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : PrimitiveSchema {
            override val type: SchemaType
                get() = SchemaType.INT
            override val name: String
                get() = "int"
        }

        data class LongSchema(
            override val originalSchema: Schema,
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : PrimitiveSchema {
            override val type: SchemaType
                get() = SchemaType.LONG
            override val name: String
                get() = "long"
        }

        data class FloatSchema(
            override val originalSchema: Schema,
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : PrimitiveSchema {
            override val type: SchemaType
                get() = SchemaType.FLOAT
            override val name: String
                get() = "float"
        }

        data class DoubleSchema(
            override val originalSchema: Schema,
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : PrimitiveSchema {
            override val type: SchemaType
                get() = SchemaType.DOUBLE
            override val name: String
                get() = "double"
        }

        data class BytesSchema(
            override val originalSchema: Schema,
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : PrimitiveSchema, ByteArraySchema {
            override val type: SchemaType
                get() = SchemaType.BYTES
            override val name: String
                get() = "bytes"
        }

        data class StringSchema(
            override val originalSchema: Schema,
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : PrimitiveSchema {
            override val type: SchemaType
                get() = SchemaType.STRING
            override val name: String
                get() = "string"
        }
    }

    data class UnionSchema(
        override val originalSchema: Schema,
        val types: List<TypeSafeSchema>,
        override val isNullable: Boolean = false,
        override val props: Map<String, Any?> = emptyMap(),
    ) : TypeSafeSchema {
        override val type: SchemaType
            get() = SchemaType.UNION
        override val name: String
            get() = "union"
    }

    sealed interface CollectionSchema : TypeSafeSchema {
        data class ArraySchema(
            override val originalSchema: Schema,
            val elementSchema: TypeSafeSchema,
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : CollectionSchema {
            override val type: SchemaType
                get() = SchemaType.ARRAY
            override val name: String
                get() = "array"
        }

        data class MapSchema(
            override val originalSchema: Schema,
            val valueSchema: TypeSafeSchema,
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : CollectionSchema {
            override val type: SchemaType
                get() = SchemaType.MAP
            override val name: String
                get() = "map"
        }
    }

    sealed interface NamedSchema : TypeSafeSchema, WithProps, WithAliases, WithDoc {
        val space: String?

        override val fullName: String
            get() = if (space != null) "$space.$name" else name

        data class RecordSchema(
            override val originalSchema: Schema,
            override val name: String,
            override val space: String?,
            val fields: List<Field>,
            override val doc: String? = null,
            override val aliases: Set<String> = emptySet(),
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : NamedSchema {
            override val type: SchemaType
                get() = SchemaType.RECORD

            data class Field(
                val name: String,
                val schema: TypeSafeSchema,
                val defaultValue: Any? = NO_DEFAULT,
                override val doc: String? = null,
                override val aliases: Set<String> = emptySet(),
                override val props: Map<String, Any?> = emptyMap(),
            ) : WithProps, WithDoc, WithAliases {
                fun hasDefaultValue() = defaultValue !== NO_DEFAULT

                companion object {
                    val NO_DEFAULT = Any()
                }
            }
        }

        data class FixedSchema(
            override val originalSchema: Schema,
            override val name: String,
            override val space: String?,
            val size: UInt,
            override val doc: String? = null,
            override val aliases: Set<String> = emptySet(),
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : NamedSchema, ByteArraySchema {
            init {
                require(size < MAX_ARRAY_VM_LIMIT.toUInt()) { "Fixed size must be greater than 0" }
            }

            override val type: SchemaType
                get() = SchemaType.FIXED
        }

        data class EnumSchema(
            override val originalSchema: Schema,
            override val name: String,
            override val space: String?,
            val symbols: Set<String>,
            val defaultSymbol: String? = null,
            override val doc: String? = null,
            override val aliases: Set<String> = emptySet(),
            override val isNullable: Boolean = false,
            override val props: Map<String, Any?> = emptyMap(),
        ) : NamedSchema {
            init {
                require(defaultSymbol == null || defaultSymbol in symbols) { "Default symbol must be one of the enum symbols" }
            }

            override val type: SchemaType
                get() = SchemaType.ENUM
        }
    }

    companion object {
        fun from(schema: String): TypeSafeSchema {
            return from(Schema.Parser().parse(schema), ReferenceContainer())
        }

        private fun from(schema: Schema, seenRecords: ReferenceContainer): TypeSafeSchema {
            val (schema, isNullable) = adaptIfNullable(schema)
            seenRecords[schema.fullName]?.let { return it }

            return when (schema.type) {
                Schema.Type.RECORD -> {
                    val fields = mutableListOf<NamedSchema.RecordSchema.Field>()
                    val recordSchema =
                        NamedSchema.RecordSchema(
                            schema,
                            name = schema.name,
                            space = schema.namespace,
                            fields = fields,
                            isNullable = isNullable,
                            doc = schema.doc,
                            aliases = schema.aliases,
                            props = schema.objectProps
                        )
                    seenRecords.add(recordSchema)
                    schema.fields.map { field ->
                        NamedSchema.RecordSchema.Field(
                            name = field.name(),
                            schema = from(field.schema(), seenRecords),
                            defaultValue =
                                if (field.hasDefaultValue()) {
                                    field.defaultVal()
                                        ?.takeIf { it != JsonProperties.NULL_VALUE }
                                } else {
                                    NamedSchema.RecordSchema.Field.NO_DEFAULT
                                },
                            doc = field.doc(),
                            aliases = field.aliases(),
                            props = field.objectProps
                        )
                    }.forEach { fields += it }
                    recordSchema
                }

                Schema.Type.ENUM ->
                    NamedSchema.EnumSchema(
                        schema,
                        name = schema.name,
                        space = schema.namespace,
                        symbols = schema.enumSymbols.toSet(),
                        defaultSymbol = schema.enumDefault,
                        isNullable = isNullable,
                        doc = schema.doc,
                        aliases = schema.aliases,
                        props = schema.objectProps
                    )

                Schema.Type.UNION -> {
                    val types = schema.types.map { from(it, seenRecords) }
                    UnionSchema(schema, types, isNullable, schema.objectProps)
                }

                Schema.Type.FIXED ->
                    NamedSchema.FixedSchema(
                        schema,
                        name = schema.name,
                        space = schema.namespace,
                        size = schema.fixedSize.toUInt(),
                        isNullable = isNullable,
                        doc = schema.doc,
                        aliases = schema.aliases,
                        props = schema.objectProps
                    )

                Schema.Type.ARRAY ->
                    CollectionSchema.ArraySchema(
                        schema,
                        elementSchema = from(schema.elementType, seenRecords),
                        isNullable = isNullable,
                        props = schema.objectProps
                    )

                Schema.Type.MAP ->
                    CollectionSchema.MapSchema(
                        schema,
                        valueSchema = from(schema.valueType, seenRecords),
                        isNullable = isNullable,
                        props = schema.objectProps
                    )

                Schema.Type.BOOLEAN -> PrimitiveSchema.BooleanSchema(schema, isNullable, schema.objectProps)
                Schema.Type.INT -> PrimitiveSchema.IntSchema(schema, isNullable, schema.objectProps)
                Schema.Type.LONG -> PrimitiveSchema.LongSchema(schema, isNullable, schema.objectProps)
                Schema.Type.FLOAT -> PrimitiveSchema.FloatSchema(schema, isNullable, schema.objectProps)
                Schema.Type.DOUBLE -> PrimitiveSchema.DoubleSchema(schema, isNullable, schema.objectProps)
                Schema.Type.STRING -> PrimitiveSchema.StringSchema(schema, isNullable, schema.objectProps)
                Schema.Type.BYTES -> PrimitiveSchema.BytesSchema(schema, isNullable, schema.objectProps)

                Schema.Type.NULL -> throw IllegalArgumentException("Top-level schema cannot be of NULL type")
            }
        }

        private fun adaptIfNullable(schema: Schema): Pair<Schema, Boolean> {
            return if (schema.isNullable) {
                if (schema.types.size == 2) {
                    schema.types.first { it.type != Schema.Type.NULL } to true
                } else {
                    Schema.createUnion(schema.types.filter { it.type != Schema.Type.NULL }) to true
                }
            } else {
                schema to false
            }
        }

        private class ReferenceContainer {
            private val references: MutableMap<String, NamedSchema.RecordSchema> = mutableMapOf()

            operator fun contains(name: String) = name in references

            operator fun get(name: String) = references[name]

            fun add(reference: NamedSchema.RecordSchema) {
                references[reference.fullName] = reference
            }
        }
    }
}