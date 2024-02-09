package com.github.avrokotlin.avro4k

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

data class RecordBuilderForTest(
    val fields: List<Any?>,
) {
    fun createRecord(schema: Schema): GenericRecord {
        val record = GenericData.Record(schema)
        fields.forEachIndexed { index, value ->
            val fieldSchema = schema.fields[index].schema()
            record.put(index, convertToAvroGenericValue(value, fieldSchema))
        }
        return record
    }
}

fun convertToAvroGenericValue(
    value: Any?,
    schema: Schema,
): Any? {
    return when (value) {
        is RecordBuilderForTest -> value.createRecord(schema)
        is Map<*, *> -> createMap(schema, value)
        is Collection<*> -> createList(schema, value)
        else -> value
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