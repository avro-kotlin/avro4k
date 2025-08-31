import kotlin.Int
import kotlin.String
import kotlin.collections.List
import kotlin.collections.Map
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
public sealed interface TestSchema {
    @JvmInline
    @Serializable
    public value class ForInt(
        public val `value`: Int,
    ) : TestSchema

    @JvmInline
    @Serializable
    public value class ForMap(
        public val `value`: Map<String, TestSchemaMapUnion?>,
    ) : TestSchema

    @JvmInline
    @Serializable
    public value class ForArray(
        public val `value`: List<TestSchemaArrayUnion?>,
    ) : TestSchema
}
