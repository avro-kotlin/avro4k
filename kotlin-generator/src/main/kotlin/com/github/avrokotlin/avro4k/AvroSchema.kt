package com.github.avrokotlin.avro4k

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import org.apache.avro.specific.SpecificData

internal interface WithProps {
    val props: Map<String, JsonElement>
}

internal interface WithDoc {
    val doc: String?
}

internal val AvroSchema.isNullable: Boolean get() = this is AvroSchema.NullSchema || this is AvroSchema.UnionSchema && isNullable

internal val AvroSchema.actualJavaClassName: String?
    get() = (this as? WithProps)?.props[SpecificData.CLASS_PROP]?.jsonPrimitive?.contentOrNull

internal val AvroSchema.logicalTypeName: String?
    get() = (this as? WithProps)?.props["logicalType"]?.jsonPrimitive?.contentOrNull

internal val AvroSchema.ArraySchema.actualElementClass: String?
    get() = props[SpecificData.ELEMENT_PROP]?.jsonPrimitive?.contentOrNull

internal val AvroSchema.MapSchema.actualKeyClass: String?
    get() = props[SpecificData.KEY_CLASS_PROP]?.jsonPrimitive?.contentOrNull

/**
 * This class is the future drop-in multiplatform replacement for the Apache Avro Schema class.
 * It is not in the core module as it is tested first here. Then it will be moved to core when ready.
 */
internal sealed interface AvroSchema {
    val fullName: String get() = simpleName
    val simpleName: String

    sealed interface PrimitiveSchema : AvroSchema, WithProps

    data class BooleanSchema(
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : PrimitiveSchema {
        init {
            ensureNotProhibitedProp("type")
        }

        override val simpleName: String
            get() = "boolean"
    }

    data class IntSchema(
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : PrimitiveSchema {
        init {
            ensureNotProhibitedProp("type")
        }

        override val simpleName: String
            get() = "int"
    }

    data class LongSchema(
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : PrimitiveSchema {
        init {
            ensureNotProhibitedProp("type")
        }

        override val simpleName: String
            get() = "long"
    }

    data class FloatSchema(
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : PrimitiveSchema {
        init {
            ensureNotProhibitedProp("type")
        }

        override val simpleName: String
            get() = "float"
    }

    data class DoubleSchema(
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : PrimitiveSchema {
        init {
            ensureNotProhibitedProp("type")
        }

        override val simpleName: String
            get() = "double"
    }

    data class BytesSchema(
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : PrimitiveSchema {
        init {
            ensureNotProhibitedProp("type")
        }

        override val simpleName: String
            get() = "bytes"
    }

    data class StringSchema(
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : PrimitiveSchema {
        init {
            ensureNotProhibitedProp("type")
        }

        override val simpleName: String
            get() = "string"
    }

    data class NullSchema(
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : AvroSchema, WithProps {
        init {
            ensureNotProhibitedProp("type")
        }

        override val simpleName: String
            get() = "null"
    }

    data class UnionSchema(
        val types: List<AvroSchema>,
    ) : AvroSchema {
        val isNullable: Boolean = types.any { it is NullSchema }
        val isSimpleNullableType: Boolean get() = types.size == 2 && isNullable
        val typesByFullName = types.associateBy { it.fullName }

        init {
            require(types.isNotEmpty()) { "Union must have at least one type" }
            require(typesByFullName.size == types.size) { "Union cannot contain duplicate types" }
        }

        override val simpleName: String
            get() = "union"
    }

    data class ArraySchema(
        val elementSchema: AvroSchema,
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : AvroSchema, WithProps {
        init {
            ensureNotProhibitedProp("type", "items")
        }

        override val simpleName: String
            get() = "array"
    }

    data class MapSchema(
        val valueSchema: AvroSchema,
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : AvroSchema, WithProps {
        init {
            ensureNotProhibitedProp("type", "values")
        }

        override val simpleName: String
            get() = "map"
    }

    sealed interface NamedSchema : AvroSchema, WithProps, WithDoc {
        val name: Name
        val aliases: Set<Name>

        override val fullName: String
            get() = name.fullName
        override val simpleName: String
            get() = name.simpleName
    }

    data class RecordSchema(
        override val name: Name,
        val fields: List<Field>,
        override val doc: String? = null,
        override val aliases: Set<Name> = emptySet(),
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : NamedSchema {
        init {
            ensureNotProhibitedProp("type", "fields", "name", "namespace", "aliases", "doc")
        }

        data class Field(
            val name: String,
            val schema: AvroSchema,
            val defaultValue: JsonElement? = null,
            override val doc: String? = null,
            val aliases: Set<String> = emptySet(),
            override val props: Map<String, JsonElement> = emptyMap(),
        ) : WithProps, WithDoc {
            init {
                require(name !in aliases) { "Field name '$name' cannot be part of aliases $aliases" }
                ensureNotProhibitedProp("name", "type", "aliases", "doc", "default")
            }
        }
    }

    data class FixedSchema(
        override val name: Name,
        val size: UInt,
        override val doc: String? = null,
        override val aliases: Set<Name> = emptySet(),
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : NamedSchema {
        init {
            ensureNotProhibitedProp("type", "size", "name", "namespace", "aliases", "doc")
        }
    }

    data class EnumSchema(
        override val name: Name,
        val symbols: Set<String>,
        val defaultSymbol: String? = null,
        override val doc: String? = null,
        override val aliases: Set<Name> = emptySet(),
        override val props: Map<String, JsonElement> = emptyMap(),
    ) : NamedSchema {
        init {
            require(defaultSymbol == null || defaultSymbol in symbols) { "Default symbol must be one of the enum symbols" }
            ensureNotProhibitedProp("type", "default", "symbols", "name", "namespace", "aliases", "doc")
        }
    }

    companion object {
        // placeholder to allow static extensions to be added like AvroSchema.fromJson()
    }
}

internal class Name {
    val fullName: String
    val simpleName: String
    val space: String?

    constructor(name: String, space: String?) {
        if (space.isNullOrEmpty()) {
            this.simpleName = name.substringAfterLast('.')
            this.space = name.substringBeforeLast('.', missingDelimiterValue = "").takeIf { it.isNotEmpty() }
        } else {
            require(!name.contains('.')) { "Name cannot contain a dot if the namespace is provided: name=$name, space=$space" }
            this.simpleName = name
            this.space = space
        }
        this.fullName = if (this.space == null) this.simpleName else "${this.space}.${this.simpleName}"
    }

    constructor(fullName: String) : this(fullName, null)

    internal fun withSpaceIfMissing(space: String?) = Name(name = simpleName, space = space ?: this.space)

    override fun toString(): String {
        return fullName
    }

    override fun equals(other: Any?): Boolean {
        if (other !is Name) return false
        return fullName == other.fullName
    }

    override fun hashCode(): Int {
        return fullName.hashCode()
    }
}

private fun WithProps.ensureNotProhibitedProp(vararg prohibited: String) {
    for (p in prohibited) {
        require(p !in props) { "properties in ${this::class.simpleName} must not contain the field '$p'. Actual value: ${props[p]}" }
    }
}