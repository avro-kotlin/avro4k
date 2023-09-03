package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class GenericAvroEncoder(
        schema: Schema,
        override val serializersModule: SerializersModule,
        override val configuration: AvroConfiguration,
) : AbstractAvroEncoder() {
    var encodedValue: Any? = null
        private set

    override fun encodeNativeValue(value: Any?) {
        encodedValue = value
    }

    override val currentUnresolvedSchema: Schema = schema
}