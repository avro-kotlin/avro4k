import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import java.time.Instant
import kotlin.jvm.JvmInline
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"string","java-class":"java.time.Instant"}""")
public value class TestSchema(
    @AvroProp("java-class", "java.time.Instant")
    @Contextual
    public val `value`: Instant,
)
