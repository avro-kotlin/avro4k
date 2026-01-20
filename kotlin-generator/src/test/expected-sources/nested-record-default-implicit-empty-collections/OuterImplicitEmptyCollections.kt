@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlin.collections.emptyList
import kotlin.collections.emptyMap
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"OuterImplicitEmptyCollections","fields":[{"name":"inner","type":{"type":"record","name":"InnerImplicitEmptyCollections","fields":[{"name":"items","type":{"type":"array","items":"string"}},{"name":"props","type":{"type":"map","values":"string"}}]},"default":{"items":[],"props":{}}}]}""")
public data class OuterImplicitEmptyCollections(
    /**
     * Default value: {items=[], props={}}
     */
    @AvroDefault("{\"items\":[],\"props\":{}}")
    public val `inner`:
            InnerImplicitEmptyCollections = InnerImplicitEmptyCollections(items = emptyList(), props = emptyMap()),
)
