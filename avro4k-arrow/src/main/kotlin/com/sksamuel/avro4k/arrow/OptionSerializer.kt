package com.sksamuel.avro4k.arrow

import arrow.core.Option
import com.sksamuel.avro4k.decoder.ExtendedDecoder
import com.sksamuel.avro4k.encoder.ExtendedEncoder
import com.sksamuel.avro4k.serializer.AvroSerializer
import kotlinx.serialization.Encoder
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializer
import org.apache.avro.Schema
import java.time.LocalDate

//class Tuple2Serializer : AvroSerializer<Tuple2<*, *>>() {
//
//   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: Tuple2<*, *>) {
//   }
//
//   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Tuple2<*, *> {
//      TODO()
//   }
//
//   override val descriptor: SerialDescriptor
//      get() = TODO()
//}
//

@Serializer(forClass = Option::class)
class OptionSerializer<A> : AvroSerializer<Option<A>>() {

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: Option<A>) {
   }

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Option<A> {
   }


}