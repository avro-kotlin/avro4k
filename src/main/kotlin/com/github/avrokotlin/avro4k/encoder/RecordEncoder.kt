package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.Record
import com.github.avrokotlin.avro4k.schema.extractNonNull
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

@ExperimentalSerializationApi
interface StructureEncoder : FieldEncoder {
    val configuration: AvroConfiguration

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.LIST -> {
                when (descriptor.getElementDescriptor(0).unwrapValueClass.kind) {
                    PrimitiveKind.BYTE -> ByteArrayEncoder(fieldSchema(), serializersModule) { addValue(it) }
                    else -> ListEncoder(fieldSchema(), serializersModule, configuration) { addValue(it) }
                }
            }
            StructureKind.CLASS -> RecordEncoder(fieldSchema(), serializersModule, configuration) { addValue(it) }
            StructureKind.MAP -> MapEncoder(fieldSchema(), serializersModule, configuration) { addValue(it) }
            is PolymorphicKind -> UnionEncoder(fieldSchema(), serializersModule, configuration) { addValue(it) }
            else -> throw SerializationException(".beginStructure was called on a non-structure type [$descriptor]")
        }
    }
}

@ExperimentalSerializationApi
internal val SerialDescriptor.unwrapValueClass: SerialDescriptor
    get() = if (isInline) getElementDescriptor(0) else this

@ExperimentalSerializationApi
class RecordEncoder(
    private val schema: Schema,
    override val serializersModule: SerializersModule,
    override val configuration: AvroConfiguration,
    val callback: (Record) -> Unit,
) : AbstractEncoder(), StructureEncoder {
    private val builder = RecordBuilder(schema)
    private var currentIndex = -1

    override fun fieldSchema(): Schema {
        // if the element is nullable, then we should have a union schema which we can extract the non-null schema from
        val currentFieldSchema = schema.fields[currentIndex].schema()
        return if (currentFieldSchema.isNullable) {
            currentFieldSchema.extractNonNull()
        } else {
            currentFieldSchema
        }
    }

    override fun addValue(value: Any) {
        builder.add(value)
    }

    override fun encodeString(value: String) {
        builder.add(StringToAvroValue.toValue(fieldSchema(), value))
    }

    override fun encodeChar(value: Char) {
        val schema = fieldSchema()
        when (schema.type) {
            Schema.Type.STRING -> builder.add(value.toString())
            Schema.Type.INT -> builder.add(value.code)
            else -> throw SerializationException("Unsupported type for Char: $schema")
        }
    }

    override fun encodeValue(value: Any) {
        builder.add(value)
    }

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        currentIndex = index
        return true
    }

    override fun encodeByteArray(buffer: ByteBuffer) {
        builder.add(buffer)
    }

    override fun encodeFixed(fixed: GenericFixed) {
        builder.add(fixed)
    }

    override fun encodeEnum(
        enumDescriptor: SerialDescriptor,
        index: Int,
    ) {
        builder.add(ValueToEnum.toValue(fieldSchema(), enumDescriptor, index))
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return super<StructureEncoder>.beginStructure(descriptor)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        callback(builder.record())
    }

    override fun encodeNull() {
        builder.add(null)
    }
}

class RecordBuilder(private val schema: Schema) {
    private val values = ArrayList<Any?>(schema.fields.size)

    fun add(value: Any?) = values.add(value)

    fun record(): Record = ListRecord(schema, values)
}