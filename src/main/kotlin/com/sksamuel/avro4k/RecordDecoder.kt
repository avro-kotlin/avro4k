package com.sksamuel.avro4k

import kotlinx.serialization.NamedValueDecoder
import org.apache.avro.generic.GenericData

class RecordDecoder(val record: GenericData.Record) : NamedValueDecoder() {

  override fun decodeTaggedDouble(tag: String): Double {
    return record.get(tag) as Double
  }

  override fun decodeTaggedLong(tag: String): Long {
    return record.get(tag) as Long
  }

  override fun decodeTaggedFloat(tag: String): Float {
    return record.get(tag) as Float
  }

  override fun decodeTaggedBoolean(tag: String): Boolean {
    return record.get(tag) as Boolean
  }

  override fun decodeTaggedInt(tag: String): Int {
    return record.get(tag) as Int
  }

  override fun decodeTaggedString(tag: String): String {
    return record.get(tag) as String
  }
}