@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package ns2

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlin.collections.List
import kotlin.collections.emptyList
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"Main","namespace":"ns2","fields":[{"name":"field","type":{"type":"array","items":[{"type":"record","name":"A","fields":[{"name":"field","type":"int"}]},{"type":"record","name":"B","fields":[{"name":"field","type":"int"}]}]}}]}""")
public data class Main(
    public val `field`: List<FieldArrayUnion> = emptyList(),
) {
    @Serializable
    @AvroGenerated("""[{"type":"record","name":"A","namespace":"ns2","fields":[{"name":"field","type":"int"}]},{"type":"record","name":"B","namespace":"ns2","fields":[{"name":"field","type":"int"}]}]""")
    public sealed interface FieldArrayUnion {
        @JvmInline
        @Serializable
        public value class ForA(
            public val `value`: A,
        ) : FieldArrayUnion

        @JvmInline
        @Serializable
        public value class ForB(
            public val `value`: B,
        ) : FieldArrayUnion
    }
}
