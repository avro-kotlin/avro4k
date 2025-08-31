import com.github.avrokotlin.avro4k.AvroDoc
import kotlin.String
import kotlinx.serialization.Serializable

@Serializable
public data class NestedRecord(
    @AvroDoc("field doc")
    public val `field`: String,
)
