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
import kotlin.String
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"BillingAddress","namespace":"ns","fields":[{"name":"street_line","type":"string","doc":"Street line","default":"unknown"}]}""")
public data class BillingAddress(
    /**
     * Street line
     *
     * Default value: unknown
     */
    @AvroDoc("Street line")
    @AvroDefault("unknown")
    @SerialName("street_line")
    public val streetLine: String = "unknown",
)
