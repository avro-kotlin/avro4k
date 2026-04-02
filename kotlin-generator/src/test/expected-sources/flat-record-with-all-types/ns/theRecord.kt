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
import kotlin.Double
import kotlin.Float
import kotlin.Int
import kotlin.Long
import kotlin.OptIn
import kotlin.String
import kotlin.collections.List
import kotlin.collections.Map
import kotlin.collections.emptyList
import kotlin.collections.emptyMap
import kotlin.collections.listOf
import kotlin.collections.mapOf
import kotlinx.serialization.Serializable

/**
 * doc
 */
@Serializable
@AvroProp("customProp", "customValue")
@AvroDoc("doc")
@AvroAlias(
    "ns.alias1",
    "ns.toto",
)
@AvroGenerated("""{"type":"record","name":"theRecord","namespace":"ns","doc":"doc","fields":[{"name":"b","type":"bytes","default":"\u0001\u0002\u0003","aliases":["fieldAlias1","fieldAlias2"],"customFieldProp":"customValue"},{"name":"i","type":"int","default":42},{"name":"l","type":"long","default":4242},{"name":"f1","type":"float","default":4.2},{"name":"d","type":"double","default":8.4},{"name":"bool","type":"boolean","default":true},{"name":"str","type":"string","default":"the default"},{"name":"nullableStringField","type":["null","string"],"default":null},{"name":"nullableStringFieldButDefaulted","type":["string","null"],"default":"default value"},{"name":"f","type":{"type":"fixed","name":"Fixed","doc":"the doc","size":3},"default":"ÿ\u0004\u0007"},{"name":"map","type":{"type":"map","values":"int"},"default":{"a":1,"b":2}},{"name":"mapWithNullableValues","type":{"type":"map","values":["null","int"]}},{"name":"array","type":{"type":"array","items":"int"},"default":[17,42]},{"name":"arrayWithNullableValues","type":{"type":"array","items":["null","int"]}}],"customProp":"customValue","aliases":["alias1","toto"]}""")
public data class theRecord(
    /**
     * Default value: [1, 2, 3]
     */
    @AvroProp("customFieldProp", "customValue")
    @AvroAlias(
        "fieldAlias1",
        "fieldAlias2",
    )
    @AvroDefault("\u0001\u0002\u0003")
    public val b: ByteArray = byteArrayOf(1, 2, 3),
    /**
     * Default value: 42
     */
    @AvroDefault("42")
    public val i: Int = 42,
    /**
     * Default value: 4242
     */
    @AvroDefault("4242")
    public val l: Long = 4_242,
    /**
     * Default value: 4.2
     */
    @AvroDefault("4.2")
    public val f1: Float = 4.2f,
    /**
     * Default value: 8.4
     */
    @AvroDefault("8.4")
    public val d: Double = 8.4,
    /**
     * Default value: true
     */
    @AvroDefault("true")
    public val bool: Boolean = true,
    /**
     * Default value: the default
     */
    @AvroDefault("the default")
    public val str: String = "the default",
    /**
     * Default value: null
     */
    @AvroDefault("null")
    public val nullableStringField: String? = null,
    /**
     * Default value: default value
     */
    @AvroDefault("default value")
    public val nullableStringFieldButDefaulted: String? = "default value",
    /**
     * Default value: [-1, 4, 7]
     */
    @AvroFixed(size = 3)
    @AvroDefault("ÿ\u0004\u0007")
    public val f: ByteArray = byteArrayOf(-1, 4, 7),
    /**
     * Default value: {"a":1,"b":2}
     */
    @AvroDefault("{\"a\":1,\"b\":2}")
    public val map: Map<String, Int> = mapOf("a" to 1, "b" to 2),
    public val mapWithNullableValues: Map<String, Int?> = emptyMap(),
    /**
     * Default value: [17,42]
     */
    @AvroDefault("[17,42]")
    public val array: List<Int> = listOf(17, 42),
    public val arrayWithNullableValues: List<Int?> = emptyList(),
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as theRecord
        if (!b.contentEquals(other.b)) return false
        if (i != other.i) return false
        if (l != other.l) return false
        if (f1 != other.f1) return false
        if (d != other.d) return false
        if (bool != other.bool) return false
        if (str != other.str) return false
        if (nullableStringField != other.nullableStringField) return false
        if (nullableStringFieldButDefaulted != other.nullableStringFieldButDefaulted) return false
        if (!f.contentEquals(other.f)) return false
        if (map != other.map) return false
        if (mapWithNullableValues != other.mapWithNullableValues) return false
        if (array != other.array) return false
        if (arrayWithNullableValues != other.arrayWithNullableValues) return false
        return true
    }

    override fun hashCode(): Int {
        var result = b.contentHashCode()
        result = 31 * result + i.hashCode()
        result = 31 * result + l.hashCode()
        result = 31 * result + f1.hashCode()
        result = 31 * result + d.hashCode()
        result = 31 * result + bool.hashCode()
        result = 31 * result + str.hashCode()
        result = 31 * result + nullableStringField.hashCode()
        result = 31 * result + nullableStringFieldButDefaulted.hashCode()
        result = 31 * result + f.contentHashCode()
        result = 31 * result + map.hashCode()
        result = 31 * result + mapWithNullableValues.hashCode()
        result = 31 * result + array.hashCode()
        result = 31 * result + arrayWithNullableValues.hashCode()
        return result
    }
}
