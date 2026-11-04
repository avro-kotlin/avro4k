@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package com.example

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable
import com.example.nl.PushNotification as NlPushNotification
import com.example.se.PushNotification as SePushNotification

@Serializable
@AvroGenerated("""{"type":"record","name":"Notification","namespace":"com.example","fields":[{"name":"value","type":[{"type":"record","name":"UnknownNotification","fields":[]},{"type":"record","name":"PushNotification","namespace":"com.example.se","fields":[{"name":"title","type":"string"}]},{"type":"record","name":"PushNotification","namespace":"com.example.nl","fields":[{"name":"title","type":"string"}]}]}]}""")
public data class Notification(
    public val `value`: ValueUnion,
) {
    @Serializable
    @AvroGenerated("""[{"type":"record","name":"UnknownNotification","namespace":"com.example","fields":[]},{"type":"record","name":"PushNotification","namespace":"com.example.se","fields":[{"name":"title","type":"string"}]},{"type":"record","name":"PushNotification","namespace":"com.example.nl","fields":[{"name":"title","type":"string"}]}]""")
    public sealed interface ValueUnion {
        @JvmInline
        @Serializable
        public value class ForUnknownNotification(
            public val `value`: UnknownNotification,
        ) : ValueUnion

        @JvmInline
        @Serializable
        public value class ForComExampleSePushNotification(
            public val `value`: SePushNotification,
        ) : ValueUnion

        @JvmInline
        @Serializable
        public value class ForComExampleNlPushNotification(
            public val `value`: NlPushNotification,
        ) : ValueUnion
    }
}
