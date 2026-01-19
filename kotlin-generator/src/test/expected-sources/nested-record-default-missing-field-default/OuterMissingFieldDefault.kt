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
@AvroGenerated("""{"type":"record","name":"OuterMissingFieldDefault","fields":[{"name":"inner","type":{"type":"record","name":"InnerMissingFieldDefault","fields":[{"name":"x","type":"string","default":""},{"name":"y","type":"int","default":42}]},"default":{"x":""}}]}""")
public data class OuterMissingFieldDefault(
    /**
     * Default value: {x=}
     */
    @AvroDefault("{\"x\":\"\"}")
    public val `inner`: InnerMissingFieldDefault = InnerMissingFieldDefault(x = "", y = 42),
)
