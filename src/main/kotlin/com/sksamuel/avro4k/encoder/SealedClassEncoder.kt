package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Record
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.builtins.AbstractEncoder
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema

class SealedClassEncoder(private val schema: Schema,
                         override val context: SerialModule,
                         private val callback: (Record) -> Unit): AbstractEncoder() {
   override fun encodeString(value: String){
      //No need to encode the string of the concrete type. This will be handled by the UnionEncoder
   }

   override fun <T : Any?> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T){
      return UnionEncoder(schema,context,callback).encodeSerializableValue(serializer,value)
   }
   override fun endStructure(descriptor: SerialDescriptor) {}
}