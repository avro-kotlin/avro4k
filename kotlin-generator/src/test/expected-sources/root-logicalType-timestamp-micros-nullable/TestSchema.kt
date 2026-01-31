@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import java.time.Instant
import kotlin.OptIn
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""[{"type":"long","logicalType":"timestamp-micros"},"null"]""")
public value class TestSchema(
    @AvroProp("logicalType", "timestamp-micros")
    public val `value`: @Serializable(with = InstantSerializer::class) Instant? = null,
)
