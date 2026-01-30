@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import java.math.BigDecimal
import kotlin.OptIn
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"NestedLogicalTypeDecimalBytes","fields":[{"name":"amount","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":4}],"doc":"A nullable decimal field","default":null}]}""")
public data class NestedLogicalTypeDecimalBytes(
    /**
     * A nullable decimal field
     *
     * Default value: null
     */
    @AvroDoc("A nullable decimal field")
    @AvroDecimal(
        scale = 4,
        precision = 10,
    )
    @AvroDefault("null")
    public val amount: @Serializable(with = BigDecimalSerializer::class) BigDecimal?,
)
