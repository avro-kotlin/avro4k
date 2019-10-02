package com.sksamuel.avro4k

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

/**
 * An implementation of [org.apache.avro.generic.GenericContainer] that implements
 * both [GenericRecord] and [SpecificRecord].
 */
interface Record : GenericRecord, SpecificRecord

data class ListRecord(val s: Schema, val values: List<Any?>) : Record {

   constructor(s: Schema, vararg values: Any?) : this(s, values.toList())

   init {
      require(schema.type == Schema.Type.RECORD) { "Cannot create an ArrayListRecord with a schema that is not a RECORD" }
   }

   override fun getSchema(): Schema = s

   override fun put(key: String, v: Any): Unit =
      throw  UnsupportedOperationException("This implementation of Record is immutable")

   override fun put(i: Int, v: Any): Unit =
      throw  UnsupportedOperationException("This implementation of Record is immutable")

   override fun get(key: String): Any? {
      val index = schema.fields.indexOfFirst { it.name() == key }
      if (index == -1)
         throw RuntimeException("Field $key does not exist in this record (schema=$schema, values=$values)")
      return get(index)
   }

   override fun get(i: Int): Any? = values[i]
}