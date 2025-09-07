import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import java.math.BigDecimal
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"fixed","name":"IgnoredName","namespace":"ignored.namespace","size":20,"logicalType":"decimal","precision":10,"scale":4}""")
public value class TestSchema(
    @AvroDecimal(
        scale = 4,
        precision = 10,
    )
    @AvroFixed(size = 20)
    @AvroProp("logicalType", "decimal")
    @AvroProp("precision", "10")
    @AvroProp("scale", "4")
    @Serializable(with = BigDecimalSerializer::class)
    public val `value`: BigDecimal,
)
