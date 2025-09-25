@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

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
    @AvroDoc("A nullable decimal field")
    @AvroDefault("null")
    @Serializable(with = BigDecimalSerializer::class)
    public val amount: BigDecimal?,
)
