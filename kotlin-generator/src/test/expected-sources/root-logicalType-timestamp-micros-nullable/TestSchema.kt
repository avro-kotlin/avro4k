import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import java.time.Instant
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""[{"type":"long","logicalType":"timestamp-micros"},"null"]""")
public value class TestSchema(
    @AvroProp("logicalType", "timestamp-micros")
    @AvroDefault("null")
    @Serializable(with = InstantSerializer::class)
    public val `value`: Instant? = null,
)
