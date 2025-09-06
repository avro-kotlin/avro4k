package ns

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import kotlin.Int
import kotlin.String
import kotlinx.serialization.Serializable

@Serializable
@AvroDoc("doc")
@AvroAlias("ns.alias2")
public data class theRecord2(
    @AvroDoc("field doc")
    public val field1: String,
    @AvroAlias(
        "field3",
        "otherField",
    )
    @AvroDefault("null")
    public val field2: Int? = null,
)
