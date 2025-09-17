@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import kotlin.Double
import kotlin.Int
import kotlin.OptIn
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
public sealed interface TestSchemaMapUnion {
    @JvmInline
    @Serializable
    public value class ForDouble(
        public val `value`: Double,
    ) : TestSchemaMapUnion

    @JvmInline
    @Serializable
    public value class ForInt(
        public val `value`: Int,
    ) : TestSchemaMapUnion
}
