import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Int
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"int","logicalType":"unknownType","unknownTypeProperty":"unknownTypeValue"}""")
public value class TestSchema(
    @AvroProp("logicalType", "unknownType")
    @AvroProp("unknownTypeProperty", "unknownTypeValue")
    public val `value`: Int,
)
