package com.github.avrokotlin.avro4k

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

/**
 * An implementation of [org.apache.avro.generic.GenericContainer] that implements
 * both [GenericRecord] and [SpecificRecord].
 */
interface Record : GenericRecord, SpecificRecord

data class ListRecord(
    private val s: Schema,
    private val values: List<Any?>
) : Record {

    constructor(s: Schema, vararg values: Any?) : this(s, values.toList())

    init {
        require(schema.type == Schema.Type.RECORD) { "Cannot create a Record with a schema that is not of type Schema.Type.RECORD [was $s]" }
    }

    override fun getSchema(): Schema = s

    override fun put(key: String, v: Any): Unit =
        throw UnsupportedOperationException("This implementation of Record is immutable")

    override fun put(i: Int, v: Any): Unit =
        throw UnsupportedOperationException("This implementation of Record is immutable")

    override fun get(key: String): Any? {
        val index = schema.fields.indexOfFirst { it.name() == key }
        if (index == -1)
            throw RuntimeException("Field $key does not exist in this record (schema=$schema, values=$values)")
        return get(index)
    }

    override fun get(i: Int): Any? = values[i]
}

fun MutableListRecord(schema: Schema, initializer: MutableListRecord.() -> Unit) = MutableListRecord(schema).apply(initializer)

class MutableListRecord(
    private val schema: Schema,
) : Record {
    private val values: Array<Any?>

    init {
        require(schema.type == Schema.Type.RECORD) { "Cannot create a Record with a schema that is not of type Schema.Type.RECORD [was $schema]" }
        values = Array(schema.fields.size) { null }
    }

    override fun getSchema() = schema

    override fun put(key: String, value: Any?) = put(schema.getField(key).pos(), value)

    override fun put(fieldPosition: Int, value: Any?) {
        values[fieldPosition] = value
    }

    operator fun set(fieldName: String, value: Any?) {
        values[schema.getField(fieldName).pos()] = value
    }

    operator fun set(fieldPosition: Int, value: Any?) {
        values[fieldPosition] = value
    }

    override operator fun get(key: String): Any? {
        return get(schema.getField(key).pos())
    }

    override operator fun get(i: Int): Any? = values[i]
}