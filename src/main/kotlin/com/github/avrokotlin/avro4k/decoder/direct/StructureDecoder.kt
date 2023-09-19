package com.github.avrokotlin.avro4k.decoder.direct

import com.github.avrokotlin.avro4k.io.AvroDecoder
import com.github.avrokotlin.avro4k.schema.Resolver
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import org.apache.avro.Schema

@ExperimentalSerializationApi
abstract class StructureDecoder : AbstractDecoder() {
    abstract val currentAction: Resolver.Action
    abstract val decoder: AvroDecoder
    override fun <T : Any?> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        val action = currentAction
        return if (action is Resolver.SetDefault) {
            @Suppress("UNCHECKED_CAST")
            action.defaultValue as T
        } else {
            super.decodeSerializableValue(deserializer)
        }
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        when (val fieldAction = currentAction) {
            is Resolver.Container -> when (descriptor.kind) {
                StructureKind.MAP -> MapDecoder(decoder, serializersModule, fieldAction)
                StructureKind.LIST -> ListDecoder(decoder, serializersModule, fieldAction)
            }
            is Resolver.ReaderUnion,
            is Resolver.WriterUnion -> PolymorphicDecoder(serializersModule, decoder, fieldAction)
            is Resolver.RecordAdjust -> RecordDecoder(
                decoder,
                serializersModule,
                fieldAction
            )
            else -> throw SerializationException("Can't perform $fieldAction on the descriptor $descriptor. Make sure that the data corresponds to the passed schemas.")
        }
        return super.beginStructure(descriptor)
    }

    override fun decodeDouble(): Double {
        return when (currentAction) {
            is Resolver.DoNothing -> decoder.readDouble()
            is Resolver.Promote -> when (currentAction.writer.type) {
                Schema.Type.INT -> decoder.readInt().toDouble()
                Schema.Type.LONG -> decoder.readLong().toDouble()
                Schema.Type.FLOAT -> decoder.readFloat().toDouble()
                else -> throw IllegalStateException("The resolved promote action is not correct.")
            }

            else -> throw UnsupportedOperationException("The resolved action $currentAction can currently not be decoded.")
        }
    }

    override fun decodeFloat(): Float {
        return when (currentAction) {
            is Resolver.DoNothing -> decoder.readFloat()
            is Resolver.Promote -> when (currentAction.writer.type) {
                Schema.Type.INT -> decoder.readInt().toFloat()
                Schema.Type.LONG -> decoder.readLong().toFloat()
                else -> throw IllegalStateException("The resolved promote action is not correct.")
            }

            else -> throw UnsupportedOperationException("The resolved action $currentAction can currently not be decoded.")
        }
    }

    override fun decodeLong(): Long {
        return when (currentAction) {
            is Resolver.DoNothing -> decoder.readLong()
            is Resolver.Promote -> if (currentAction.writer.type == Schema.Type.INT) decoder.readInt()
                .toLong() else throw IllegalStateException("The resolved promote action is not correct.")

            else -> throw UnsupportedOperationException("The resolved action $currentAction can currently not be decoded.")
        }
    }

    override fun decodeString(): String {
        return when (currentAction) {
            is Resolver.DoNothing -> decoder.readString()
            is Resolver.Promote -> if (currentAction.writer.type == Schema.Type.BYTES) decoder.readString() else throw IllegalStateException(
                "The resolved promote action is not correct."
            )

            else -> throw UnsupportedOperationException("The resolved action $currentAction can currently not be decoded.")
        }
    }

    override fun decodeNotNullMark(): Boolean {
        val action = currentAction
        return if (action is Resolver.ReaderUnion) {
            action.reader.types[action.firstMatch].type != Schema.Type.NULL
        } else if (action is Resolver.WriterUnion) {
            //Only handle case where it is a nullable primitive. All other cases will be handled by the UnionDecoder
            val schemaIndex = decoder.readIndex()
            if (action.unionEquiv) {
                action.reader.types[schemaIndex].type != Schema.Type.NULL
            } else {
                val decodingAction = action.actions[schemaIndex] as Resolver.ReaderUnion
                decodingAction.reader.types[decodingAction.firstMatch].type != Schema.Type.NULL
            }
        } else {
            true
        }
    }

    override fun decodeBoolean(): Boolean {
        return decoder.readBoolean()
    }

    override fun decodeByte(): Byte {
        return decoder.readInt().toByte()
    }

    override fun decodeChar(): Char {
        return decoder.readInt().toChar()
    }

    override fun decodeInt(): Int {
        return decoder.readInt()
    }

    override fun decodeShort(): Short {
        return decoder.readInt().toShort()
    }
}