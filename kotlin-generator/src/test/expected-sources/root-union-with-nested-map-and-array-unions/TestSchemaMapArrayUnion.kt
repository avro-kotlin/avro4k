@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Boolean
import kotlin.Long
import kotlin.OptIn
import kotlin.String
import kotlin.collections.Map
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""["long","null",{"type":"map","values":["boolean","null"]}]""")
public sealed interface TestSchemaMapArrayUnion {
    @AvroAlias("long")
    @JvmInline
    @Serializable
    public value class ForLong(
        public val `value`: Long,
    ) : TestSchemaMapArrayUnion

    @AvroAlias("map")
    @JvmInline
    @Serializable
    public value class ForMap(
        public val `value`: Map<String, Boolean?>,
    ) : TestSchemaMapArrayUnion
}
