package com.github.avrokotlin.avro4k.internal

import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.LogicalType
import org.apache.avro.Schema

internal val Schema.nullable: Schema
    get() {
        if (isNullable) return this
        return if (isUnion) {
            Schema.createUnion(listOf(Schema.create(Schema.Type.NULL)) + this.types)
        } else {
            Schema.createUnion(Schema.create(Schema.Type.NULL), this)
        }
    }

internal fun Schema.asSchemaList(): List<Schema> {
    if (!isUnion) return listOf(this)
    return types
}

internal fun Schema.isNamedSchema(): Boolean {
    return this.type == Schema.Type.RECORD || this.type == Schema.Type.ENUM || this.type == Schema.Type.FIXED
}

internal fun Schema.isFullNameOrAliasMatch(descriptor: SerialDescriptor): Boolean {
    return isFullNameOrAliasMatch(descriptor.nonNullSerialName, descriptor::aliases)
}

internal fun Schema.isFullNameOrAliasMatch(
    fullName: String,
    aliases: () -> Set<String>,
): Boolean {
    return isFullNameMatch(fullName) || aliases().any { isFullNameMatch(it) }
}

internal fun Schema.isFullNameMatch(fullNameToMatch: String): Boolean {
    return fullName == fullNameToMatch ||
        (type == Schema.Type.RECORD || type == Schema.Type.ENUM || type == Schema.Type.FIXED) &&
        aliases.any { it == fullNameToMatch }
}

private val SCHEMA_PLACEHOLDER = Schema.create(Schema.Type.NULL)

internal fun Schema.copy(
    name: String = this.name,
    doc: String? = this.doc,
    namespace: String? = if (this.type == Schema.Type.RECORD || this.type == Schema.Type.ENUM || this.type == Schema.Type.FIXED) this.namespace else null,
    aliases: Set<String> = if (this.type == Schema.Type.RECORD || this.type == Schema.Type.ENUM || this.type == Schema.Type.FIXED) this.aliases else emptySet(),
    isError: Boolean = if (this.type == Schema.Type.RECORD) this.isError else false,
    types: List<Schema> = if (this.isUnion) this.types.toList() else emptyList(),
    enumSymbols: List<String> = if (this.type == Schema.Type.ENUM) this.enumSymbols.toList() else emptyList(),
    fields: List<Schema.Field>? = if (this.type == Schema.Type.RECORD && this.hasFields()) this.fields.map { it.copy() } else null,
    enumDefault: String? = if (this.type == Schema.Type.ENUM) this.enumDefault else null,
    fixedSize: Int = if (this.type == Schema.Type.FIXED) this.fixedSize else -1,
    valueType: Schema = if (this.type == Schema.Type.MAP) this.valueType else SCHEMA_PLACEHOLDER,
    elementType: Schema = if (this.type == Schema.Type.ARRAY) this.elementType else SCHEMA_PLACEHOLDER,
    objectProps: Map<String, Any> = this.objectProps,
    additionalProps: Map<String, Any> = emptyMap(),
    logicalType: LogicalType? = this.logicalType,
): Schema {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (type) {
        Schema.Type.RECORD -> if (hasFields()) Schema.createRecord(name, doc, namespace, isError, fields) else Schema.createRecord(name, doc, namespace, isError)
        Schema.Type.ENUM -> Schema.createEnum(name, doc, namespace, enumSymbols, enumDefault)
        Schema.Type.FIXED -> Schema.createFixed(name, doc, namespace, fixedSize)

        Schema.Type.UNION -> Schema.createUnion(types)
        Schema.Type.MAP -> Schema.createMap(valueType)
        Schema.Type.ARRAY -> Schema.createArray(elementType)
        Schema.Type.BYTES,
        Schema.Type.STRING,
        Schema.Type.INT,
        Schema.Type.LONG,
        Schema.Type.FLOAT,
        Schema.Type.DOUBLE,
        Schema.Type.BOOLEAN,
        Schema.Type.NULL,
        -> Schema.create(type)
    }
        .also { newSchema ->
            objectProps.forEach { (key, value) -> newSchema.addProp(key, value) }
            additionalProps.forEach { (key, value) -> newSchema.addProp(key, value) }
            logicalType?.addToSchema(newSchema)
            aliases.forEach { newSchema.addAlias(it) }
        }
}

internal fun Schema.Field.copy(
    name: String = this.name(),
    schema: Schema = this.schema(),
    doc: String? = this.doc(),
    defaultVal: Any? = this.defaultVal(),
    order: Schema.Field.Order = this.order(),
    aliases: Set<String> = this.aliases(),
    objectProps: Map<String, Any> = this.objectProps,
    additionalProps: Map<String, Any> = emptyMap(),
): Schema.Field {
    return Schema.Field(name, schema, doc, defaultVal, order)
        .also { newSchema ->
            objectProps.forEach { (key, value) -> newSchema.addProp(key, value) }
            additionalProps.forEach { (key, value) -> newSchema.addProp(key, value) }
            aliases.forEach { newSchema.addAlias(it) }
        }
}