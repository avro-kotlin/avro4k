import com.example.CustomLogicalType
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.jvm.JvmInline
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"string","logicalType":"contextualLogicalType"}""")
public value class TestSchema(
    @AvroProp("logicalType", "contextualLogicalType")
    @Contextual
    public val `value`: CustomLogicalType,
)
