import kotlin.Int
import kotlin.String
import kotlinx.serialization.Serializable

@Serializable
public data class NestedRecord(
    public val id: String,
    public val `value`: Int,
)
