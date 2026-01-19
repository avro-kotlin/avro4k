@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Int
import kotlin.OptIn
import kotlin.String
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"InnerMissingFieldDefault","fields":[{"name":"x","type":"string","default":""},{"name":"y","type":"int","default":42}]}""")
public data class InnerMissingFieldDefault(
    /**
     * Default value: 
     */
    @AvroDefault("")
    public val x: String = "",
    /**
     * Default value: 42
     */
    @AvroDefault("42")
    public val y: Int = 42,
)
