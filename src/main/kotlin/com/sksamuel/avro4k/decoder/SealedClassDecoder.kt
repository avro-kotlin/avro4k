package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.RecordNaming
import com.sksamuel.avro4k.leavesOfSealedClasses
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

@ExperimentalSerializationApi
class SealedClassDecoder (descriptor: SerialDescriptor,
                          private val value: GenericRecord,
                          override val serializersModule: SerializersModule
) : AbstractDecoder(), FieldDecoder
{
   private enum class DecoderState(val index : Int){
      BEFORE(0),
      READ_CLASS_NAME(1),
      READ_DONE(CompositeDecoder.DECODE_DONE);
      fun next() = values().firstOrNull{ it.ordinal > this.ordinal }?:READ_DONE
   }
   private var currentState = DecoderState.BEFORE

   var leafDescriptor : SerialDescriptor = descriptor.leavesOfSealedClasses().firstOrNull {
      val schemaName = RecordNaming(value.schema.fullName, emptyList())
      val serialName = RecordNaming(it)
      serialName.name() == schemaName.name() && serialName.namespace() == schemaName.namespace()
   }?:throw SerializationException("Cannot find a subtype of ${descriptor.serialName} that can be used to deserialize a record of schema ${value.schema}.")

   override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
      val currentIndex = currentState.index
      currentState = currentState.next()
      return currentIndex
   }

   override fun fieldSchema(): Schema = value.schema

   /**
    * Decode string needs to return the class name of the actual decoded class.
    */
   override fun decodeString(): String {
      return leafDescriptor.serialName
   }

   override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
      val recordDecoder = RootRecordDecoder(value, serializersModule)
      return recordDecoder.decodeSerializableValue(deserializer)
   }

   override fun decodeAny(): Any? = UnsupportedOperationException()
}