@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.example.CustomLogicalType
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"SubRecordWithKSerializer","fields":[{"name":"field","type":{"type":"string","logicalType":"customLogicalTypeWithKSerializer"}}]}""")
public data class SubRecordWithKSerializer(
    public val `field`:
            @Serializable(with = CustomLogicalType.TheNestedSerializer::class) CustomLogicalType,
)
