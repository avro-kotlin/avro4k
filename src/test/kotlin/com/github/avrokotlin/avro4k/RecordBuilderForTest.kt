package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.extractNonNull
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

data class RecordBuilderForTest(
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

fun convertToAvroGenericValue(
    value: Any?,
    schema: Schema,
): Any? {
    val schema = if (schema.isNullable) schema.extractNonNull() else schema
    return when (value) {
        is RecordBuilderForTest -> value.createRecord(schema)
        is Map<*, *> -> createMap(schema, value)
        is Collection<*> -> createList(schema, value)
        is ByteArray -> if (schema.type == Schema.Type.FIXED) GenericData.Fixed(schema, value) else ByteBuffer.wrap(value)
        is String -> if (schema.type == Schema.Type.ENUM) GenericData.get().createEnum(value, schema) else value
        else -> value
    }
}

fun normalizeGenericData(value: Any?): Any? {
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
        is GenericEnumSymbol<*> -> value.toString()
        is CharSequence -> value.toString()

        is RecordBuilderForTest -> RecordBuilderForTest(value.fields.map { normalizeGenericData(it) })
        is Byte -> value.toInt()
        is Short -> value.toInt()
        is Boolean, is Char, is Number, null -> value

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

fun record(vararg fields: Any?): RecordBuilderForTest {
    return RecordBuilderForTest(listOf(*fields))
}

fun recordWithSchema(
    schema: Schema,
    vararg fields: Any?,
): RecordBuilderForTest {
    return RecordBuilderForTest(listOf(*fields), schema)
}