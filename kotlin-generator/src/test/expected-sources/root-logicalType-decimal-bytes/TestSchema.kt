import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import java.math.BigDecimal
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"bytes","logicalType":"decimal"}""")
public value class TestSchema(
    @AvroProp("logicalType", "decimal")
    @Serializable(with = BigDecimalSerializer::class)
    public val `value`: BigDecimal,
)
