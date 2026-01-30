@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import java.time.Instant
import kotlin.OptIn
import kotlin.jvm.JvmInline
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"string","java-class":"java.time.Instant"}""")
public value class TestSchema(
    @AvroProp("java-class", "java.time.Instant")
    public val `value`: @Contextual Instant,
)
