@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package ns1

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.internal.AvroGenerated
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"A","namespace":"ns1","fields":[{"name":"field","type":"int"}]}""")
public data class A(
    public val `field`: Int,
)
