package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Record
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class SealedClassEncoder(private val schema: Schema,
                         override val serializersModule: SerializersModule,
                         private val callback: (Record) -> Unit): AbstractEncoder() {
   override fun encodeString(value: String){
      //No need to encode the string of the concrete type. This will be handled by the UnionEncoder
   }

   override fun <T : Any?> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T){
      return UnionEncoder(schema,serializersModule,callback).encodeSerializableValue(serializer,value)
   }
   override fun endStructure(descriptor: SerialDescriptor) {}
}