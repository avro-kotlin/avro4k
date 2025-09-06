import com.github.avrokotlin.avro4k.AvroDefault
import kotlin.String
import kotlin.collections.List
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
public data class ComplexUnionInRecord(
    @AvroDefault("null")
    public val theField: TheFieldUnion? = null,
) {
    @Serializable
    public sealed interface TheFieldUnion {
        @JvmInline
        @Serializable
        public value class ForNestedRecord(
            public val `value`: NestedRecord,
        ) : TheFieldUnion

        @JvmInline
        @Serializable
        public value class ForStatus(
            public val `value`: Status,
        ) : TheFieldUnion

        @JvmInline
        @Serializable
        public value class ForArray(
            public val `value`: List<String>,
        ) : TheFieldUnion
    }
}
