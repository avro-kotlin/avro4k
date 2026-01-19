@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"deep","type":{"type":"record","name":"Deep","fields":[{"name":"x","type":"string","default":""}]},"default":{"x":""}}]},"default":{"deep":{"x":""}}}]}""")
public data class Outer(
    /**
     * Default value: {deep={x=}}
     */
    @AvroDefault("{\"deep\":{\"x\":\"\"}}")
    public val `inner`: Inner = Inner(deep = Deep(x = "")),
)
