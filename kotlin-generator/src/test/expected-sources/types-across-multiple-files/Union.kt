@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.ByteArray
import kotlin.OptIn
import kotlin.String
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable
import ns.Record
import ns2.Enum

@Serializable
@AvroGenerated("""[{"type":"record","name":"Record","namespace":"ns","fields":[{"name":"field","type":"int"}]},{"type":"record","name":"RecordWithoutNamespace","fields":[{"name":"field","type":"int"}]},{"type":"enum","name":"Enum","namespace":"ns2","symbols":["FIRST","SECOND"]},{"type":"fixed","name":"FixedType","size":12},"string","bytes"]""")
public sealed interface Union {
    @JvmInline
    @Serializable
    public value class ForRecord(
        public val `value`: Record,
    ) : Union

    @JvmInline
    @Serializable
    public value class ForRecordWithoutNamespace(
        public val `value`: RecordWithoutNamespace,
    ) : Union

    @JvmInline
    @Serializable
    public value class ForEnum(
        public val `value`: Enum,
    ) : Union

    @JvmInline
    @Serializable
    public value class ForFixedType(
        @AvroFixed(size = 12)
        public val `value`: ByteArray,
    ) : Union

    @JvmInline
    @Serializable
    public value class ForString(
        public val `value`: String,
    ) : Union

    @JvmInline
    @Serializable
    public value class ForBytes(
        public val `value`: ByteArray,
    ) : Union
}
