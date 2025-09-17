@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package ns

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Serializable

@Serializable
@AvroProp("customProp", "customValue")
@AvroDoc("doc")
@AvroAlias(
    "ns.alias1",
    "ns.alias2",
)
@AvroGenerated("""{"type":"enum","name":"theEnum","namespace":"ns","doc":"doc","symbols":["A","B","C"],"default":"B","customProp":"customValue","aliases":["alias1","alias2"]}""")
public enum class theEnum {
    A,
    @AvroEnumDefault
    B,
    C,
}
