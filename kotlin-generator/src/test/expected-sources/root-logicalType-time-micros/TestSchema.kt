import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import com.github.avrokotlin.avro4k.serializer.LocalTimeSerializer
import java.time.LocalTime
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"long","logicalType":"time-micros"}""")
public value class TestSchema(
    @AvroProp("logicalType", "time-micros")
    @Serializable(with = LocalTimeSerializer::class)
    public val `value`: LocalTime,
)
