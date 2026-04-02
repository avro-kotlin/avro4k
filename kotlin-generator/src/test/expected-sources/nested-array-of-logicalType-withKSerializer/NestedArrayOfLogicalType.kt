@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.example.CustomLogicalType
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlin.collections.List
import kotlin.collections.emptyList
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"NestedArrayOfLogicalType","fields":[{"name":"amounts","type":{"type":"array","items":{"type":"string","logicalType":"customLogicalTypeWithKSerializer"}},"doc":"An array of types having a registered serializer, so needs to add @Serializable(with = CustomLogicalTypeWithKSerializer::class) annotation on the parameterized list's type and not the field"}]}""")
public data class NestedArrayOfLogicalType(
    /**
     * An array of types having a registered serializer, so needs to add @Serializable(with = CustomLogicalTypeWithKSerializer::class) annotation on the parameterized list's type and not the field
     */
    @AvroDoc("An array of types having a registered serializer, so needs to add @Serializable(with = CustomLogicalTypeWithKSerializer::class) annotation on the parameterized list's type and not the field")
    public val amounts:
            List<@Serializable(with = CustomLogicalType.TheNestedSerializer::class) CustomLogicalType> = emptyList(),
)
