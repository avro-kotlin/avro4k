package com.github.avrokotlin.avro4k.encoder.direct

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
import kotlinx.serialization.serializer
import org.apache.avro.Schema


val byteArraySerializer = serializer<ByteArray>()
@ExperimentalSerializationApi
abstract class StructureEncoder : AbstractEncoder(), DirectFieldEncoder {

   override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
      handleNullability()
      if(serializer.descriptor == byteArraySerializer.descriptor) {
         encodeByteArray(value as ByteArray)
      }else {
         super<AbstractEncoder>.encodeSerializableValue(serializer, value)
      }
   }
   fun encodeByteArray(byteArray: ByteArray) {
      val fieldSchema = fieldSchema()
      val relevantSchema =  fieldSchema.nonNullSchema
      if (relevantSchema.type == Schema.Type.FIXED) {
         avroEncoder.writeFixedWithPadding(byteArray, relevantSchema.fixedSize)
      } else {
         avroEncoder.writeBytes(byteArray)
      }
   }

   override fun beginCollection(descriptor: SerialDescriptor, collectionSize: Int): CompositeEncoder {
      val schema = fieldSchema()
      return when(schema.type){
         Schema.Type.ARRAY -> ListEncoder(schema.nonNullSchema, serializersModule, avroEncoder, collectionSize)
         Schema.Type.MAP -> MapEncoder(schema.nonNullSchema, serializersModule, avroEncoder, collectionSize)
         else -> super<AbstractEncoder>.beginCollection(descriptor, collectionSize)
      }
   }

   override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
      return when (descriptor.kind) {
         StructureKind.CLASS -> RecordEncoder(fieldSchema().nonNullSchema, serializersModule, avroEncoder)
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
      doWriteValue.invoke()
   }

   private fun handleNullability() {
      val fieldSchema = fieldSchema()
      val nonNullSchemaIndex = fieldSchema.nonNullSchemaIndex
      if (nonNullSchemaIndex != -1) {
         avroEncoder.writeUnionSchema(fieldSchema, nonNullSchemaIndex)
      }
   }

   override fun encodeBoolean(value: Boolean) {
      avroEncoder.writeBoolean(value)
   }

   override fun encodeByte(value: Byte) {
      avroEncoder.writeInt(value.toInt())
   }

   override fun encodeChar(value: Char) {
      avroEncoder.writeInt(value.code)
   }

   override fun encodeDouble(value: Double) {
      avroEncoder.writeDouble(value)
   }

   override fun encodeFloat(value: Float) {
      avroEncoder.writeFloat(value)
   }

   override fun encodeInt(value: Int) {
      avroEncoder.writeInt(value)
   }

   override fun encodeLong(value: Long) {
      avroEncoder.writeLong(value)
   }

   override fun encodeShort(value: Short) {
      avroEncoder.writeInt(value.toInt())
   }
}