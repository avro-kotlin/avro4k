@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package ns1

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Serializable
import ns2.TheEnum

@Serializable
@AvroGenerated("""{"type":"record","name":"NestedEnum","namespace":"ns1","fields":[{"name":"theValue","type":[{"type":"enum","name":"TheEnum","namespace":"ns2","symbols":["A","B","C"],"default":"C"},"null"],"default":"B"}]}""")
public data class NestedEnum(
    /**
     * Default value: B
     */
    @AvroDefault("B")
    public val theValue: TheEnum? = TheEnum.B,
)
