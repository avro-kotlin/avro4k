package com.github.avrokotlin.avro4k.encoder.direct

import com.github.avrokotlin.avro4k.io.AvroEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class DirectRootRecordEncoder(
    private val schema: Schema,
    override val serializersModule: SerializersModule,
    override val avroEncoder: AvroEncoder
) : StructureEncoder() {
    override fun fieldSchema(): Schema = schema
}