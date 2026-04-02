@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Int
import kotlin.OptIn
import kotlin.collections.Map
import kotlin.collections.emptyMap
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@JvmInline
@Serializable
@AvroGenerated("""{"type":"map","values":["double","null","int"],"java-key-class":"java.lang.Integer"}""")
public value class TestSchema(
    @AvroProp("java-key-class", "java.lang.Integer")
    public val `value`: Map<Int, TestSchemaMapUnion?> = emptyMap(),
)
