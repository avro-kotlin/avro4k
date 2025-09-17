@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import kotlin.Double
import kotlin.Long
import kotlin.OptIn
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
public sealed interface TestSchemaArrayUnion {
    @JvmInline
    @Serializable
    public value class ForLong(
        public val `value`: Long,
    ) : TestSchemaArrayUnion

    @JvmInline
    @Serializable
    public value class ForDouble(
        public val `value`: Double,
    ) : TestSchemaArrayUnion
}
