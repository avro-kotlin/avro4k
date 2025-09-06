import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Int
import kotlin.collections.Map
import kotlin.collections.emptyMap
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"map","values":["double","null","int"],"java-key-class":"java.lang.Integer"}""")
public value class TestSchema(
    @AvroProp("java-key-class", "java.lang.Integer")
    @AvroDefault("{}")
    public val `value`: Map<Int, TestSchemaMapUnion?> = emptyMap(),
)
