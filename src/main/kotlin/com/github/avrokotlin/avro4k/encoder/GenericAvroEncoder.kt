package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.ExperimentalSerializationApi
import org.apache.avro.Schema

@ExperimentalSerializationApi
class GenericAvroEncoder(
    schema: Schema,
    override val avro: Avro,
) : AbstractAvroEncoder() {
    var encodedValue: Any? = null
        private set

    override fun encodeNativeValue(value: Any?) {
        encodedValue = value
    }

    override val currentUnresolvedSchema: Schema = schema
}