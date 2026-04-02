@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import java.time.Instant
import kotlin.OptIn
import kotlin.collections.List
import kotlin.collections.emptyList
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"NestedArrayOfLogicalType","fields":[{"name":"amounts","type":{"type":"array","items":{"type":"string","java-class":"java.time.Instant"}},"doc":"An array of types that are not serializable natively, so it requires @Contextual annotation of the parameterized list's type and not the field"}]}""")
public data class NestedArrayOfLogicalType(
    /**
     * An array of types that are not serializable natively, so it requires @Contextual annotation of the parameterized list's type and not the field
     */
    @AvroDoc("An array of types that are not serializable natively, so it requires @Contextual annotation of the parameterized list's type and not the field")
    public val amounts: List<@Contextual Instant> = emptyList(),
)
