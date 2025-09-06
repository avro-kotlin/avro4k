import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
import java.time.LocalDate
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"int","logicalType":"date"}""")
public value class TestSchema(
    @AvroProp("logicalType", "date")
    @Serializable(with = LocalDateSerializer::class)
    public val `value`: LocalDate,
)
