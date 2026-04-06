@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package ns

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Serializable

/**
 * doc
 */
@Serializable
@AvroDoc("doc")
@AvroGenerated("""{"type":"record","name":"theNestedUnion","namespace":"ns","doc":"doc","fields":[{"name":"s","type":["null","theNestedUnion"],"default":null}]}""")
public data class theNestedUnion(
    /**
     * Default value: null
     */
    @AvroDefault("null")
    public val s: theNestedUnion? = null,
)
