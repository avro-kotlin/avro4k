import kotlin.Boolean
import kotlin.Long
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
