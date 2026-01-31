@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlin.String
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"Test","fields":[{"name":"field","type":"string"}]}""")
public data class Test(
    public val `field`: String,
)
