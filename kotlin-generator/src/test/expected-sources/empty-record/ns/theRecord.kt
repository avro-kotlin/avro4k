package ns

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlinx.serialization.Serializable

@Serializable
@AvroProp("customProp", "customValue")
@AvroDoc("doc")
@AvroAlias(
    "ns.alias1",
    "ns.toto",
)
@AvroGenerated("""{"type":"record","name":"theRecord","namespace":"ns","doc":"doc","fields":[],"customProp":"customValue","aliases":["alias1","toto"]}""")
public object theRecord
