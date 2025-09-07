import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.String
import kotlin.collections.List
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"ComplexUnionInRecord","fields":[{"name":"theField","type":["null",{"type":"record","name":"NestedRecord","fields":[{"name":"id","type":"string"},{"name":"value","type":"int"}]},{"type":"enum","name":"Status","symbols":["ACTIVE","INACTIVE","PENDING"]},{"type":"array","items":"string"}],"default":null}]}""")
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
