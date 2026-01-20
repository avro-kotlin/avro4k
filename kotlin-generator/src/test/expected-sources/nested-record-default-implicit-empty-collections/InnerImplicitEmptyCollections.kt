@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlin.String
import kotlin.collections.List
import kotlin.collections.Map
import kotlin.collections.emptyList
import kotlin.collections.emptyMap
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"InnerImplicitEmptyCollections","fields":[{"name":"items","type":{"type":"array","items":"string"}},{"name":"props","type":{"type":"map","values":"string"}}]}""")
public data class InnerImplicitEmptyCollections(
    public val items: List<String> = emptyList(),
    public val props: Map<String, String> = emptyMap(),
)
