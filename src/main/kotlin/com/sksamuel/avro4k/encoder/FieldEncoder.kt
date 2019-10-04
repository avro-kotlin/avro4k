package com.sksamuel.avro4k.encoder

import org.apache.avro.Schema

interface FieldEncoder {
   fun addValue(value: Any)
   fun fieldSchema(): Schema
}