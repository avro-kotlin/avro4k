@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Any
import kotlin.Boolean
import kotlin.ByteArray
import kotlin.Float
import kotlin.Int
import kotlin.OptIn
import kotlin.String
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"SimpleSubRecord","fields":[{"name":"intField","type":"int"},{"name":"floatField","type":"float"},{"name":"stringField","type":"string"},{"name":"bytesField","type":"bytes"},{"name":"fixedField","type":{"type":"fixed","name":"Fixed","size":3}},{"name":"enumField","type":{"type":"enum","name":"Enum","symbols":["A","B"]}},{"name":"recordField","type":{"type":"record","name":"DeepRecord","fields":[{"name":"field","type":"string"}]}}]}""")
public data class SimpleSubRecord(
    public val intField: Int,
    public val floatField: Float,
    public val stringField: String,
    public val bytesField: ByteArray,
    @AvroFixed(size = 3)
    public val fixedField: ByteArray,
    public val enumField: Enum,
    public val recordField: DeepRecord,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as SimpleSubRecord
        if (intField != other.intField) return false
        if (floatField != other.floatField) return false
        if (stringField != other.stringField) return false
        if (!bytesField.contentEquals(other.bytesField)) return false
        if (!fixedField.contentEquals(other.fixedField)) return false
        if (enumField != other.enumField) return false
        if (recordField != other.recordField) return false
        return true
    }

    override fun hashCode(): Int {
        var result = intField.hashCode()
        result = 31 * result + floatField.hashCode()
        result = 31 * result + stringField.hashCode()
        result = 31 * result + bytesField.contentHashCode()
        result = 31 * result + fixedField.contentHashCode()
        result = 31 * result + enumField.hashCode()
        result = 31 * result + recordField.hashCode()
        return result
    }
}
