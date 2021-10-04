package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.RecordNaming
import com.github.avrokotlin.avro4k.possibleSerializationSubclasses
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

@ExperimentalSerializationApi
class UnionDecoder (descriptor: SerialDescriptor,
                    private val value: GenericRecord,
                    override val serializersModule: SerializersModule,
                    private  val configuration: AvroConfiguration
) : AbstractDecoder(), FieldDecoder
{

   private var leafDescriptor : SerialDescriptor = descriptor.possibleSerializationSubclasses(serializersModule).firstOrNull {
      val schemaName = RecordNaming(value.schema.fullName, emptyList())
      val serialName = RecordNaming(it)
      serialName == schemaName
   }?:throw SerializationException("Cannot find a subtype of ${descriptor.serialName} that can be used to deserialize a record of schema ${value.schema}.")

   override fun decodeElementIndex(descriptor: SerialDescriptor): Int = -1

   @ExperimentalSerializationApi
   override fun decodeSequentially() = true

   override fun fieldSchema(): Schema = value.schema

   /**
    * Decode string needs to return the class name of the actual decoded class.
    */
   override fun decodeString(): String {
      return leafDescriptor.serialName
   }

   override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
      val recordDecoder = RootRecordDecoder(value, serializersModule, configuration)
      return recordDecoder.decodeSerializableValue(deserializer)
   }

   override fun decodeAny(): Any = UnsupportedOperationException()
}
