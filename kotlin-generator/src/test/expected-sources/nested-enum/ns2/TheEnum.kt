@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package ns2

import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"enum","name":"TheEnum","namespace":"ns2","symbols":["A","B","C"],"default":"C"}""")
public enum class TheEnum {
    A,
    B,
    @AvroEnumDefault
    C,
}
