@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import kotlin.OptIn
import kotlin.String
import kotlin.collections.List
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
public sealed interface TestSchemaMapUnion {
    @JvmInline
    @Serializable
    public value class ForString(
        public val `value`: String,
    ) : TestSchemaMapUnion

    @JvmInline
    @Serializable
    public value class ForArray(
        public val `value`: List<TestSchemaMapArrayUnion?>,
    ) : TestSchemaMapUnion
}
