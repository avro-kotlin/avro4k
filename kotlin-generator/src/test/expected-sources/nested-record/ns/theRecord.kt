package ns

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.AvroProp
import kotlin.Any
import kotlin.Boolean
import kotlin.ByteArray
import kotlin.Int
import kotlin.String
import kotlin.collections.List
import kotlin.collections.Map
import kotlinx.serialization.Serializable

@Serializable
@AvroProp("customProp", "customValue")
@AvroDoc("doc")
@AvroAlias(
    "ns.alias1",
    "ns.toto",
)
public data class theRecord(
    public val s: theRecord,
    @AvroDoc("field doc")
    public val `field`: theRecord2,
    @AvroDefault("\u0001\u0002\u0003")
    public val b: ByteArray = byteArrayOf(1, 2, 3),
    @AvroFixed(size = 3)
    @AvroDefault("ÿ\u0004\u0007")
    public val f: ByteArray = byteArrayOf(-1, 4, 7),
    @AvroDefault("{\"a\":1,\"b\":2}")
    public val map: Map<String, Int> = mapOf("a" to 1, "b" to 2),
    @AvroDefault("[17,42]")
    public val array: List<Int> = listOf(17, 42),
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as theRecord
        if (s != other.s) return false
        if (`field` != other.`field`) return false
        if (!b.contentEquals(other.b)) return false
        if (!f.contentEquals(other.f)) return false
        if (map != other.map) return false
        if (array != other.array) return false
        return true
    }

    override fun hashCode(): Int {
        var result = s.hashCode()
        result = 31 * result + `field`.hashCode()
        result = 31 * result + b.contentHashCode()
        result = 31 * result + f.contentHashCode()
        result = 31 * result + map.hashCode()
        result = 31 * result + array.hashCode()
        return result
    }
}
