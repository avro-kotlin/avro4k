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
