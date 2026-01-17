@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlin.String
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"Deep","fields":[{"name":"x","type":"string","default":""}]}""")
public data class Deep(
    /**
     * Default value: 
     */
    @AvroDefault("")
    public val x: String = "",
)
