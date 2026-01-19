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
@AvroGenerated("""{"type":"record","name":"Inner","fields":[{"name":"deep","type":{"type":"record","name":"Deep","fields":[{"name":"x","type":"string","default":""}]},"default":{"x":""}}]}""")
public data class Inner(
    /**
     * Default value: {x=}
     */
    @AvroDefault("{\"x\":\"\"}")
    public val deep: Deep = Deep(x = ""),
)
