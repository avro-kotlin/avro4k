import kotlin.Double
import kotlin.Long
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
