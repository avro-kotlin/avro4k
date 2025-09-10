import com.example.CustomLogicalType
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"string","logicalType":"customLogicalTypeWithKSerializer"}""")
public value class TestSchema(
    @AvroProp("logicalType", "customLogicalTypeWithKSerializer")
    @Serializable(with = CustomLogicalType.TheNestedSerializer::class)
    public val `value`: CustomLogicalType,
)
