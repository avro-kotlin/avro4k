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
import kotlin.collections.List
import kotlin.collections.Map
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"Notification","namespace":"com.example","fields":[{"name":"value","type":[{"type":"record","name":"UnknownNotification","fields":[]},{"type":"record","name":"EmailNotification","fields":[{"name":"to","type":"string"}]}],"default":{}},{"name":"nullableValue","type":["null","string","int"],"default":null},{"name":"primitive","type":["string","int"],"default":"abc"},{"name":"arrayOfUnion","type":{"type":"array","items":["string","int"]},"default":["abc"]},{"name":"mapOfUnion","type":{"type":"map","values":["string","int"]},"default":{"key":"abc"}},{"name":"nested","type":{"type":"record","name":"Holder","fields":[{"name":"inner","type":["int","string"],"default":1}]},"default":{"inner":7}}]}""")
public data class Notification(
    /**
     * Default value: {}
     */
    @AvroDefault("{}")
    public val `value`: ValueUnion = ValueUnion.ForUnknownNotification(UnknownNotification),
    /**
     * Default value: null
     */
    @AvroDefault("null")
    public val nullableValue: NullableValueUnion? = null,
    /**
     * Default value: abc
     */
    @AvroDefault("abc")
    public val primitive: PrimitiveUnion = PrimitiveUnion.ForString("abc"),
    /**
     * Default value: ["abc"]
     */
    @AvroDefault("[\"abc\"]")
    public val arrayOfUnion: List<ArrayOfUnionArrayUnion>,
    /**
     * Default value: {"key":"abc"}
     */
    @AvroDefault("{\"key\":\"abc\"}")
    public val mapOfUnion: Map<String, MapOfUnionMapUnion>,
    /**
     * Default value: {"inner":7}
     */
    @AvroDefault("{\"inner\":7}")
    public val nested: Holder = Holder(`inner` = Holder.InnerUnion.ForInt(7)),
) {
    @Serializable
    @AvroGenerated("""[{"type":"record","name":"UnknownNotification","namespace":"com.example","fields":[]},{"type":"record","name":"EmailNotification","namespace":"com.example","fields":[{"name":"to","type":"string"}]}]""")
    public sealed interface ValueUnion {
        @JvmInline
        @Serializable
        public value class ForUnknownNotification(
            public val `value`: UnknownNotification,
        ) : ValueUnion

        @JvmInline
        @Serializable
        public value class ForEmailNotification(
            public val `value`: EmailNotification,
        ) : ValueUnion
    }

    @Serializable
    @AvroGenerated("""["null","string","int"]""")
    public sealed interface NullableValueUnion {
        @JvmInline
        @Serializable
        public value class ForString(
            public val `value`: String,
        ) : NullableValueUnion

        @JvmInline
        @Serializable
        public value class ForInt(
            public val `value`: Int,
        ) : NullableValueUnion
    }

    @Serializable
    @AvroGenerated("""["string","int"]""")
    public sealed interface PrimitiveUnion {
        @JvmInline
        @Serializable
        public value class ForString(
            public val `value`: String,
        ) : PrimitiveUnion

        @JvmInline
        @Serializable
        public value class ForInt(
            public val `value`: Int,
        ) : PrimitiveUnion
    }

    @Serializable
    @AvroGenerated("""["string","int"]""")
    public sealed interface ArrayOfUnionArrayUnion {
        @JvmInline
        @Serializable
        public value class ForString(
            public val `value`: String,
        ) : ArrayOfUnionArrayUnion

        @JvmInline
        @Serializable
        public value class ForInt(
            public val `value`: Int,
        ) : ArrayOfUnionArrayUnion
    }

    @Serializable
    @AvroGenerated("""["string","int"]""")
    public sealed interface MapOfUnionMapUnion {
        @JvmInline
        @Serializable
        public value class ForString(
            public val `value`: String,
        ) : MapOfUnionMapUnion

        @JvmInline
        @Serializable
        public value class ForInt(
            public val `value`: Int,
        ) : MapOfUnionMapUnion
    }
}
