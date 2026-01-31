@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Double
import kotlin.OptIn
import kotlin.collections.List
import kotlin.collections.emptyList
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"array","items":["double","null","int"],"java-element-class":"java.lang.Double"}""")
public value class TestSchema(
    @AvroProp("java-element-class", "java.lang.Double")
    public val `value`: List<Double?> = emptyList(),
)
