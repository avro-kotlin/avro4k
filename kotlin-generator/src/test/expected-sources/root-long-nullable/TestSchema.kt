import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Long
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""["long","null"]""")
public value class TestSchema(
    @AvroDefault("null")
    public val `value`: Long? = null,
)
