import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import java.util.UUID
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"string","logicalType":"uuid"}""")
public value class TestSchema(
    @AvroProp("logicalType", "uuid")
    @Serializable(with = UUIDSerializer::class)
    public val `value`: UUID,
)
