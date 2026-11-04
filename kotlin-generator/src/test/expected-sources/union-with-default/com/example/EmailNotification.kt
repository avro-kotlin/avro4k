@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package com.example

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlin.String
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"EmailNotification","namespace":"com.example","fields":[{"name":"to","type":"string"}]}""")
public data class EmailNotification(
    public val to: String,
)
