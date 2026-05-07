@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Int
import kotlin.OptIn
import kotlin.String
import kotlin.collections.List
import kotlin.collections.Map
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""["int","null",{"type":"map","values":["string","null",{"type":"array","items":["long","null",{"type":"map","values":["boolean","null"]}]}]},{"type":"array","items":["long","null","double"]}]""")
public sealed interface TestSchema {
    @JvmInline
    @Serializable
    public value class ForInt(
        public val `value`: Int,
    ) : TestSchema

    @JvmInline
    @Serializable
    public value class ForMap(
        public val `value`: Map<String, TestSchemaMapUnion?>,
    ) : TestSchema

    @JvmInline
    @Serializable
    public value class ForArray(
        public val `value`: List<TestSchemaArrayUnion?>,
    ) : TestSchema
}
