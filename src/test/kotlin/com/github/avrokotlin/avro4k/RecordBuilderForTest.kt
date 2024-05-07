package com.github.avrokotlin.avro4k

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

internal data class RecordBuilderForTest(
    val fields: List<Any?>,
    val explicitSchema: Schema? = null,
) {
    fun createRecord(schema: Schema): GenericRecord {
        val record = GenericData.Record(explicitSchema ?: schema)
        fields.forEachIndexed { index, value ->
            val fieldSchema = record.schema.fields[index].schema()
            record.put(index, convertToAvroGenericValue(value, fieldSchema))
        }
        return record
    }
}

internal fun convertToAvroGenericValue(
    value: Any?,
    schema: Schema,
): Any? {
    val schema = schema.nonNull
    return when (value) {
        is RecordBuilderForTest -> value.createRecord(schema)
        is Map<*, *> -> createMap(schema, value)
        is Collection<*> -> createList(schema, value)
        is ByteArray -> if (schema.type == Schema.Type.FIXED) GenericData.Fixed(schema, value) else ByteBuffer.wrap(value)
        is String -> if (schema.type == Schema.Type.ENUM) GenericData.get().createEnum(value, schema) else value
        else -> value
    }
}

internal fun normalizeGenericData(value: Any?): Any? {
    return when (value) {
        is IndexedRecord ->
            RecordBuilderForTest(
                value.schema.fields.map { field ->
                    normalizeGenericData(value[field.pos()])
                }
            )

        is Map<*, *> -> value.entries.associate { it.key.toString() to normalizeGenericData(it.value) }
        is Collection<*> -> value.map { normalizeGenericData(it) }
        is ByteArray -> value.toList()
        is ByteBuffer -> value.array().toList()
        is GenericFixed -> value.bytes().toList()
        is CharSequence -> value.toString()

        is RecordBuilderForTest -> RecordBuilderForTest(value.fields.map { normalizeGenericData(it) })
        is Byte -> value.toInt()
        is Short -> value.toInt()
        is GenericEnumSymbol<*>,
        is Boolean, is Char, is Number, null,
        -> value

        else -> TODO("Not implemented for ${value.javaClass}")
    }
}

private fun createList(
    schema: Schema,
    value: Collection<*>,
): List<*> {
    val valueSchema = schema.elementType
    return value.map { convertToAvroGenericValue(it, valueSchema) }
}

private fun <K, V> createMap(
    schema: Schema,
    value: Map<K, V>,
): Map<K, *> {
    val valueSchema = schema.valueType
    return value.mapValues { convertToAvroGenericValue(it.value, valueSchema) }
}

internal fun record(vararg fields: Any?): RecordBuilderForTest {
    return RecordBuilderForTest(listOf(*fields))
}

internal fun recordWithSchema(
    schema: Schema,
    vararg fields: Any?,
): RecordBuilderForTest {
    return RecordBuilderForTest(listOf(*fields), schema)
}

private val Schema.nonNull: Schema
    get() =
        when {
            type == Schema.Type.UNION && isNullable -> this.types.filter { it.type != Schema.Type.NULL }.let { if (it.size > 1) Schema.createUnion(it) else it[0] }
            else -> this
        }