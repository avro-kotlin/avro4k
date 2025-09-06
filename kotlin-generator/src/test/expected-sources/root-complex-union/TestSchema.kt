import com.github.avrokotlin.avro4k.AvroFixed
import kotlin.ByteArray
import kotlin.Double
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
    public value class ForString(
        public val `value`: String,
    ) : TestSchema

    @JvmInline
    @Serializable
    public value class ForInt(
        public val `value`: Int,
    ) : TestSchema

    @JvmInline
    @Serializable
    public value class ForNestedRecord(
        public val `value`: NestedRecord,
    ) : TestSchema

    @JvmInline
    @Serializable
    public value class ForAnEnum(
        public val `value`: AnEnum,
    ) : TestSchema

    @JvmInline
    @Serializable
    public value class ForAFixed(
        @AvroFixed(size = 5)
        public val `value`: ByteArray,
    ) : TestSchema

    @JvmInline
    @Serializable
    public value class ForArray(
        public val `value`: List<Int>,
    ) : TestSchema

    @JvmInline
    @Serializable
    public value class ForMap(
        public val `value`: Map<String, Double?>,
    ) : TestSchema
}
