package com.sksamuel.avro4k.encoder

import kotlinx.serialization.Encoder
import org.apache.avro.Schema

interface FieldEncoder : Encoder {
   fun addValue(value: Any)
   fun fieldSchema(): Schema
}