@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"Test","fields":[{"name":"nullableId","type":["null",{"type":"record","name":"Id","fields":[{"name":"value","type":"string"}]}]},{"name":"nonNullId","type":"Id","doc":"a previous bug made this field nullable"}]}""")
public data class Test(
    public val nullableId: Id? = null,
    /**
     * a previous bug made this field nullable
     */
    @AvroDoc("a previous bug made this field nullable")
    public val nonNullId: Id,
)
