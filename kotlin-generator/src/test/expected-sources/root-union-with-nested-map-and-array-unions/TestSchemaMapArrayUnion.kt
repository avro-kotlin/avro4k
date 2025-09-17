@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import kotlin.Boolean
import kotlin.Long
import kotlin.OptIn
import kotlin.String
import kotlin.collections.Map
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
public sealed interface TestSchemaMapArrayUnion {
    @JvmInline
    @Serializable
    public value class ForLong(
        public val `value`: Long,
    ) : TestSchemaMapArrayUnion

    @JvmInline
    @Serializable
    public value class ForMap(
        public val `value`: Map<String, Boolean?>,
    ) : TestSchemaMapArrayUnion
}
