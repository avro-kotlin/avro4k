@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package ns1

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlin.String
import kotlin.collections.Map
import kotlin.collections.emptyMap
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"Main","namespace":"ns1","fields":[{"name":"field","type":{"type":"map","values":[{"type":"record","name":"A","fields":[{"name":"field","type":"int"}]},{"type":"record","name":"B","fields":[{"name":"field","type":"int"}]}]}}]}""")
public data class Main(
    public val `field`: Map<String, FieldMapUnion> = emptyMap(),
) {
    @Serializable
    @AvroGenerated("""[{"type":"record","name":"A","namespace":"ns1","fields":[{"name":"field","type":"int"}]},{"type":"record","name":"B","namespace":"ns1","fields":[{"name":"field","type":"int"}]}]""")
    public sealed interface FieldMapUnion {
        @JvmInline
        @Serializable
        public value class ForA(
            public val `value`: A,
        ) : FieldMapUnion

        @JvmInline
        @Serializable
        public value class ForB(
            public val `value`: B,
        ) : FieldMapUnion
    }
}
