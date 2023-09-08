package com.github.avrokotlin.avro4k.encoder.direct

import com.github.avrokotlin.avro4k.AnnotationExtractor
import com.github.avrokotlin.avro4k.io.AvroEncoder
import com.github.avrokotlin.avro4k.schema.nonNullSchema
import com.github.avrokotlin.avro4k.schema.nonNullSchemaIndex
import com.github.avrokotlin.avro4k.schema.nullSchemaIndex
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import org.apache.avro.Schema

val byteArraySerializer = serializer<ByteArray>()
@ExperimentalSerializationApi
abstract class StructureEncoder : AbstractEncoder(), DirectFieldEncoder {

   override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
      if(serializer.descriptor == byteArraySerializer.descriptor) {
         encodeByteArray(value as ByteArray)
      }else {
         super<AbstractEncoder>.encodeSerializableValue(serializer, value)
      }
   }
   fun encodeByteArray(byteArray: ByteArray) {
      val fieldSchema = fieldSchema()
      val relevantSchema =  if(fieldSchema.type == Schema.Type.UNION) {
         fieldSchema.types.first { it.type != Schema.Type.NULL }
      } else {
         fieldSchema
      }
      if (relevantSchema.type == Schema.Type.FIXED) {
         avroEncoder.writeFixedWithPadding(byteArray, relevantSchema.fixedSize)
      } else {
         avroEncoder.writeBytes(byteArray)
      }
   }
   override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
      handleNullability()
      return when (descriptor.kind) {
         StructureKind.LIST -> ListEncoder(fieldSchema().nonNullSchema, serializersModule, avroEncoder)
         StructureKind.CLASS -> RecordEncoder(fieldSchema().nonNullSchema, serializersModule, avroEncoder)
         StructureKind.MAP -> MapEncoder(fieldSchema().nonNullSchema, serializersModule, avroEncoder) 
         is PolymorphicKind -> UnionEncoder(fieldSchema(), serializersModule, avroEncoder)
         else -> throw SerializationException(".beginStructure was called on a non-structure type [$descriptor]")
      }
   }
   override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
      avroEncoder.writeEnum(fieldSchema().nonNullSchema, enumDescriptor, index)
   }
   override fun encodeNull() {
      val currentFieldSchema = fieldSchema()
      val nullIndex = currentFieldSchema.nullSchemaIndex
      if(nullIndex == -1) throw IllegalArgumentException("Cannot encode null value for non nullable schema field.")
      avroEncoder.writeUnionSchema(currentFieldSchema, nullIndex)
      avroEncoder.writeNull()
   }
   override fun encodeString(value: String) = writeNonNullValue {
      avroEncoder.writeString(fieldSchema().nonNullSchema, value)
   }
   protected open fun writeNonNullValue(doWriteValue: () -> Unit) {
      handleNullability()
      doWriteValue.invoke()
   }

   private fun handleNullability() {
      val fieldSchema = fieldSchema()
      val nonNullSchemaIndex = fieldSchema.nonNullSchemaIndex
      if (nonNullSchemaIndex != -1) {
         avroEncoder.writeUnionSchema(fieldSchema, nonNullSchemaIndex)
      }
   }

   override fun encodeBoolean(value: Boolean) = writeNonNullValue {
      avroEncoder.writeBoolean(value)
   }

   override fun encodeByte(value: Byte) = writeNonNullValue {
      avroEncoder.writeInt(value.toInt())
   }

   override fun encodeChar(value: Char) = writeNonNullValue {
      avroEncoder.writeInt(value.code)
   }

   override fun encodeDouble(value: Double) = writeNonNullValue {
      avroEncoder.writeDouble(value)
   }

   override fun encodeFloat(value: Float) = writeNonNullValue {
      avroEncoder.writeFloat(value)
   }

   override fun encodeInt(value: Int) = writeNonNullValue {
      avroEncoder.writeInt(value)
   }

   override fun encodeLong(value: Long) = writeNonNullValue {
      avroEncoder.writeLong(value)
   }

   override fun encodeShort(value: Short) = writeNonNullValue {
      avroEncoder.writeInt(value.toInt())
   }


}

@ExperimentalSerializationApi
class RecordEncoder(private val schema: Schema,
                    override val serializersModule: SerializersModule,
                    override val avroEncoder: AvroEncoder) : StructureEncoder() {

   private var currentIndex = -1
   private lateinit var fieldSchema : Schema
   override fun fieldSchema(): Schema = fieldSchema
   
   override fun encodeElement(descriptor: SerialDescriptor, index: Int): Boolean {
      currentIndex = index
      fieldSchema = schema.fields[currentIndex].schema()
      return true
   }

   
   override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
      // if we have a value type, then we don't want to begin a new structure
      return if (AnnotationExtractor(descriptor.annotations).valueType())
         this
      else
         super.beginStructure(descriptor)
   }

   override fun endStructure(descriptor: SerialDescriptor) {
      // no op
   }
}
