@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.example.CustomLogicalType
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"SubRecordWithContextualSerializer","fields":[{"name":"field","type":{"type":"string","logicalType":"contextualLogicalType"}}]}""")
public data class SubRecordWithContextualSerializer(
    public val `field`: @Contextual CustomLogicalType,
)
