package com.github.avrokotlin.avro4k

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

data class RecordBuilderForTest(
    val fields: List<Any?>
) {
    fun createRecord(schema: Schema): GenericRecord {
        val record = GenericData.Record(schema)
        fields.forEachIndexed { index, value ->
            val fieldSchema = schema.fields[index].schema()
            record.put(index, convertValue(value, fieldSchema))
        }
        return record
    }

    private fun convertValue(
        value: Any?, schema: Schema
    ): Any? {
        return when (value) {
            is RecordBuilderForTest -> value.createRecord(schema)
            is Map<*, *> -> createMap(schema, value)
            is List<*> -> createList(schema, value)
            else -> value
        }
    }

    fun createList(schema: Schema, value: List<*>): List<*> {
        val valueSchema =
            if (schema.type == Schema.Type.ARRAY) schema.elementType
            else if (schema.type == Schema.Type.UNION) schema.types.find { it.type == Schema.Type.ARRAY }!!.elementType
            else throw IllegalArgumentException("No matching array schema found.")
        return value.map { convertValue(it, valueSchema) }
    }

    fun <K, V> createMap(schema: Schema, value: Map<K, V>): Map<K, *> {
        val valueSchema = if (schema.type == Schema.Type.MAP) {
            schema.valueType
        } else if (schema.type == Schema.Type.UNION) {
            schema.types.find { it.type == Schema.Type.MAP }!!.valueType
        } else throw IllegalArgumentException("No matching schema found for MAP")
        return value.mapValues { convertValue(it.value, valueSchema) }
    }
}

fun record(vararg fields: Any?): RecordBuilderForTest {
    return RecordBuilderForTest(listOf(*fields))
}