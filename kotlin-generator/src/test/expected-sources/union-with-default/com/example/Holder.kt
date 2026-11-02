@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package com.example

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Int
import kotlin.OptIn
import kotlin.String
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"Holder","namespace":"com.example","fields":[{"name":"inner","type":["int","string"],"default":1}]}""")
public data class Holder(
    /**
     * Default value: 1
     */
    @AvroDefault("1")
    public val `inner`: InnerUnion = InnerUnion.ForInt(1),
) {
    @Serializable
    @AvroGenerated("""["int","string"]""")
    public sealed interface InnerUnion {
        @JvmInline
        @Serializable
        public value class ForInt(
            public val `value`: Int,
        ) : InnerUnion

        @JvmInline
        @Serializable
        public value class ForString(
            public val `value`: String,
        ) : InnerUnion
    }
}
