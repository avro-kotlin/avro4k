@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import kotlin.OptIn
import kotlinx.serialization.Serializable

@Serializable
public enum class Status {
    ACTIVE,
    INACTIVE,
    PENDING,
}
