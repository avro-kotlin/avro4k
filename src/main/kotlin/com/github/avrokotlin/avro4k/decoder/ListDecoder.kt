package com.github.avrokotlin.avro4k.decoder


import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.schema.ensureOfType
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder.Companion.DECODE_DONE
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class ListDecoder(
   schema: Schema,
   private val list: List<Any?>,
   override val serializersModule: SerializersModule,
   override val configuration: AvroConfiguration,
) : AvroStructureDecoder() {
   init {
       schema.ensureOfType(Schema.Type.ARRAY)
   }

   private var currentIndex = -1

   override fun decodeElementIndex(descriptor: SerialDescriptor) =
       if(++currentIndex < list.size) currentIndex else DECODE_DONE

   override fun decodeAny() =
       list[currentIndex]!!

    override fun decodeNotNullMark()=
        list[currentIndex] != null

   override val currentSchema: Schema = schema.elementType

   override fun decodeCollectionSize(descriptor: SerialDescriptor): Int = list.size
}
