import kotlin.Double
import kotlin.Int
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
