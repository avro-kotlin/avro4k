package com.sksamuel.avro4k.arrow

import arrow.core.Option
import arrow.core.Tuple2
import com.sksamuel.avro4k.decoder.ExtendedDecoder
import com.sksamuel.avro4k.encoder.ExtendedEncoder
import com.sksamuel.avro4k.schema.AvroDescriptor
import com.sksamuel.avro4k.serializer.AvroSerializer
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.net.URL
import kotlin.reflect.jvm.jvmName

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
//class OptionSerializer : AvroSerializer<Option<*>> {
//
//   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: Option<*>) {
//      obj.fold(
//         { encoder.encodeNull() },
//         {}
//      )
//
//   }
//
//   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Option<*> {
//   }
//
//   override val descriptor: SerialDescriptor = object : AvroDescriptor(Option::class.jvmName, PrimitiveKind.STRING) {
//      override fun schema(annos: List<Annotation>): Schema = SchemaBuilder.builder().stringType()
//   }
//
//}