@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package ns

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Any
import kotlin.Boolean
import kotlin.ByteArray
import kotlin.Int
import kotlin.OptIn
import kotlin.String
import kotlin.collections.List
import kotlin.collections.Map
import kotlin.collections.listOf
import kotlin.collections.mapOf
import kotlinx.serialization.Serializable

@Serializable
@AvroProp("customProp", "customValue")
@AvroDoc("doc")
@AvroAlias(
    "ns.alias1",
    "ns.toto",
)
@AvroGenerated("""{"type":"record","name":"theRecord","namespace":"ns","doc":"doc","fields":[{"name":"s","type":"theRecord"},{"name":"field","type":{"type":"record","name":"theRecord2","doc":"doc","fields":[{"name":"field1","type":"string","doc":"field doc"},{"name":"field2","type":["null","int"],"default":null,"aliases":["field3","otherField"]}],"aliases":["alias2"]},"doc":"field doc"},{"name":"b","type":"bytes","default":"\u0001\u0002\u0003"},{"name":"f","type":{"type":"fixed","name":"Fixed","doc":"the doc","size":3},"default":"ÿ\u0004\u0007"},{"name":"map","type":{"type":"map","values":"int"},"default":{"a":1,"b":2}},{"name":"array","type":{"type":"array","items":"int"},"default":[17,42]}],"customProp":"customValue","aliases":["alias1","toto"]}""")
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
