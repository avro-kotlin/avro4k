@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import kotlin.OptIn
import kotlin.String
import kotlinx.serialization.Serializable

@Serializable
public data class NestedRecord(
    @AvroDoc("field doc")
    public val `field`: String,
)
