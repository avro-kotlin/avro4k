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
@AvroGenerated("""{"type":"record","name":"Test","fields":[{"name":"id","type":{"type":"record","name":"Id","fields":[{"name":"name","type":"string","default":""},{"name":"age","type":"int","default":-1}]},"default":{"name":"","age":-1}}]}""")
public data class Test(
    /**
     * Default value: {name=, age=-1}
     */
    @AvroDefault("{\"name\":\"\",\"age\":-1}")
    public val id: Id = Id(name = "", age = -1),
)
