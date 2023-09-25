package com.github.avrokotlin.avro4k.decoder.direct

import com.github.avrokotlin.avro4k.decoder.FieldDecoder
import com.github.avrokotlin.avro4k.io.AvroDecoder
import com.github.avrokotlin.avro4k.schema.Resolver
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.serializer
import org.apache.avro.Schema

val byteArraySerializer = serializer<ByteArray>()
val arrayByteSerializer = serializer<Array<Byte>>()
val listByteSerializer = serializer<List<Byte>>()

@ExperimentalSerializationApi
abstract class StructureDecoder : AbstractDecoder(), FieldDecoder {
    abstract var currentAction: Resolver.Action
    abstract val decoder: AvroDecoder

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        val action = currentAction
        return if (action is Resolver.SetDefault && action.reader.logicalType == null) {
            action.decodeDefault(deserializer)
        } else if (deserializer.descriptor == byteArraySerializer.descriptor) {
            decodeByteArray() as T
        } else if (deserializer.descriptor == arrayByteSerializer.descriptor) {
            decodeByteArray().toTypedArray() as T
        } else if (deserializer.descriptor == listByteSerializer.descriptor) {
            decodeByteArray().toList() as T
        } else {
            super<AbstractDecoder>.decodeSerializableValue(deserializer)
        }
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        var fieldAction = currentAction
        if (fieldAction is Resolver.WriterUnion) {
            fieldAction = decodeWrittenUnionSchema()
            currentAction = fieldAction
        }
        return when (fieldAction) {
            is Resolver.Container -> when (descriptor.kind) {
                StructureKind.MAP -> MapDecoder(decoder, serializersModule, fieldAction)
                StructureKind.LIST -> ListDecoder(decoder, serializersModule, fieldAction)
                else -> throw SerializationException("Can't perform $fieldAction on the descriptor $descriptor. It must be either a map or list structure.")
            }

            is Resolver.ReaderUnion -> PolymorphicDecoder(serializersModule, decoder, fieldAction)
            is Resolver.RecordAdjust -> RecordDecoder(decoder, serializersModule, fieldAction)

            else -> throw SerializationException("Can't perform $fieldAction on the descriptor $descriptor. Make sure that the data corresponds to the passed schemas.")
        }
    }

    private val Resolver.Action.isNotNull: Boolean
        get() = !(this is Resolver.ReaderUnion && reader.types[firstMatch].type != Schema.Type.NULL) && !(this is Resolver.SetDefault && this.isDefaultNull)
    private val Resolver.ReaderUnion.isSimpleKotlinNullableType: Boolean
        get() = reader.types.size <= 2 && reader.isNullable

    private fun decodeWrittenUnionSchema(): Resolver.Action {
        return when (val action = currentAction) {
            is Resolver.WriterUnion -> action.actions[decoder.readIndex()]
            else -> action
        }
    }

    override fun decodeNotNullMark(): Boolean {
        val resolvedAction = decodeWrittenUnionSchema()
        val isNotNull = resolvedAction.isNotNull
        currentAction = if (resolvedAction is Resolver.ReaderUnion && resolvedAction.isSimpleKotlinNullableType) {
            //Unwrap because nullability has been handled by this method
            resolvedAction.actualAction
        } else {
            resolvedAction
        }
        return isNotNull
    }

    override fun decodeByteArray() : ByteArray = handlePrimitiveDefault {
        return when (currentAction) {
            is Resolver.DoNothing -> when (currentAction.reader.type) {
                Schema.Type.FIXED -> decoder.readFixed(currentAction.reader.fixedSize)
                Schema.Type.BYTES -> decoder.readBytes()
                else -> throw SerializationException("Schema has not been resolved correctly. A ByteArray can only be decoded from FIXED and BYTES if it is not promoted. Actual written type: ${currentAction.writer.type}.")
            }

            is Resolver.Promote -> when (currentAction.writer.type) {
                Schema.Type.STRING -> decoder.readBytes()
                else -> throw SerializationException("Schema has not been resolved correctly. Cannot promote anything other than STRING to BYTES. Actual written type: ${currentAction.writer.type}")
            }

            else -> throw SerializationException("Can't read byte array for action: $currentAction.")
        }
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val action = currentAction
        if (action !is Resolver.EnumAdjust) throw SerializationException("Schema does not specify an enum. Cannot decode it as such.")
        val decodeIndex = decoder.readEnum()
        return if (action.noAdjustmentsNeeded) {
            decodeIndex
        } else {
            action.adjustments[decodeIndex]
        }
    }

    override fun decodeDouble(): Double = handlePrimitiveDefault {
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

    override fun decodeFloat(): Float = handlePrimitiveDefault {
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

    override fun decodeLong(): Long = handlePrimitiveDefault {
        return when (currentAction) {
            is Resolver.DoNothing -> decoder.readLong()
            is Resolver.Promote -> if (currentAction.writer.type == Schema.Type.INT) decoder.readInt()
                .toLong() else throw IllegalStateException("The resolved promote action is not correct.")

            else -> throw UnsupportedOperationException("The resolved action $currentAction can currently not be decoded.")
        }
    }

    override fun decodeString(): String = handlePrimitiveDefault {
        return when (val action = currentAction) {
            is Resolver.DoNothing -> doDecodeString()
            is Resolver.Promote -> if (action.writer.type == Schema.Type.BYTES) doDecodeString() else throw SerializationException(
                "The resolved promote action is not correct."
            )

            else -> throw SerializationException("The resolved action $currentAction can currently not be decoded.")
        }
    }

    private fun doDecodeString(): String {
        return if (currentAction.reader.type == Schema.Type.FIXED) {
            decoder.readFixedString(currentAction.reader.fixedSize.toLong())
        } else {
            decoder.readString()
        }
    }


    override fun decodeBoolean(): Boolean = handlePrimitiveDefault {
        return decoder.readBoolean()
    }

    override fun decodeByte(): Byte = handlePrimitiveDefault {
        return decoder.readInt().toByte()
    }

    override fun decodeChar(): Char = handlePrimitiveDefault {
        return decoder.readInt().toChar()
    }

    override fun decodeInt(): Int = handlePrimitiveDefault {
        return decoder.readInt()
    }

    override fun decodeShort(): Short = handlePrimitiveDefault {
        return decoder.readInt().toShort()
    }

    inline fun <reified T> handlePrimitiveDefault(doDecode: () -> T): T {
        val action = currentAction
        return if (action is Resolver.SetDefault) {
            action.decodeDefault()
        } else {
            doDecode.invoke()
        }
    }

    override fun fieldSchema(): Schema = currentAction.reader
}