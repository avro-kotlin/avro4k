@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"InnerImplicitNull","fields":[{"name":"deep","type":["null",{"type":"record","name":"DeepImplicitNull","fields":[{"name":"x","type":"string"}]}],"default":null}]}""")
public data class InnerImplicitNull(
    /**
     * Default value: null
     */
    @AvroDefault("null")
    public val deep: DeepImplicitNull? = null,
)
