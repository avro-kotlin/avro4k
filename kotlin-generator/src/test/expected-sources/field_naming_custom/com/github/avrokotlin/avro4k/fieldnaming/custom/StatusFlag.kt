@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package com.github.avrokotlin.avro4k.fieldnaming.custom

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"enum","name":"StatusFlag","namespace":"com.github.avrokotlin.avro4k.fieldnaming.custom","symbols":["ACTIVE","INACTIVE"]}""")
public enum class StatusFlag {
    ACTIVE,
    INACTIVE,
}
