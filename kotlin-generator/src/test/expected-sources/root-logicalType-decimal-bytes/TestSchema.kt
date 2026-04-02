@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import java.math.BigDecimal
import kotlin.OptIn
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"bytes","logicalType":"decimal","precision":10,"scale":4}""")
public value class TestSchema(
    @AvroDecimal(
        scale = 4,
        precision = 10,
    )
    @AvroProp("logicalType", "decimal")
    @AvroProp("precision", "10")
    @AvroProp("scale", "4")
    public val `value`: @Serializable(with = BigDecimalSerializer::class) BigDecimal,
)
