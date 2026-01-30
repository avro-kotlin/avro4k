@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.example.CustomLogicalType
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"string","logicalType":"customLogicalTypeWithKSerializer"}""")
public value class TestSchema(
    @AvroProp("logicalType", "customLogicalTypeWithKSerializer")
    public val `value`:
            @Serializable(with = CustomLogicalType.TheNestedSerializer::class) CustomLogicalType,
)
