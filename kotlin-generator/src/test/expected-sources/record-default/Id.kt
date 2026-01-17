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
@AvroGenerated("""{"type":"record","name":"Id","fields":[{"name":"name","type":"string","default":""},{"name":"age","type":"int","default":-1}]}""")
public data class Id(
    /**
     * Default value: 
     */
    @AvroDefault("")
    public val name: String = "",
    /**
     * Default value: -1
     */
    @AvroDefault("-1")
    public val age: Int = -1,
)
