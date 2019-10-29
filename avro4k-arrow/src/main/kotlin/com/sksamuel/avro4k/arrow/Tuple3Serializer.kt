package com.sksamuel.avro4k.arrow

import arrow.core.Tuple3
import com.sksamuel.avro4k.schema.AvroDescriptor
import com.sksamuel.avro4k.schema.NamingStrategy
import com.sksamuel.avro4k.schema.schemaFor
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializer
import kotlinx.serialization.StructureKind
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

@Serializer(forClass = Tuple3::class)
class Tuple3Serializer<A : Any, B : Any, C : Any>(private val serializerA: KSerializer<A>,
                                                  private val serializerB: KSerializer<B>,
                                                  private val serializerC: KSerializer<C>) : KSerializer<Tuple3<A, B, C>> {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(Tuple3::class, StructureKind.CLASS) {

      override fun schema(annos: List<Annotation>,
                          context: SerialModule,
                          namingStrategy: NamingStrategy): Schema {
         return SchemaBuilder.record("Tuple3")
            .fields()
            .name("a")
            .type(schemaFor(context, serializerA.descriptor, annos, namingStrategy).schema())
            .noDefault()
            .name("b")
            .type(schemaFor(context, serializerB.descriptor, annos, namingStrategy).schema())
            .noDefault()
            .name("c")
            .type(schemaFor(context, serializerC.descriptor, annos, namingStrategy).schema())
            .noDefault()
            .endRecord()
      }

      override fun getElementDescriptor(index: Int): SerialDescriptor {
         return when (index) {
            0 -> serializerA.descriptor
            1 -> serializerB.descriptor
            2 -> serializerC.descriptor
            else -> throw IndexOutOfBoundsException("$index is not a valid index for Tuple3")
         }
      }

      override fun getElementName(index: Int): String {
         return when (index) {
            0 -> "a"
            1 -> "b"
            2 -> "c"
            else -> throw IndexOutOfBoundsException("$index is not a valid index for Tuple3")
         }
      }
   }

   override fun serialize(encoder: Encoder, obj: Tuple3<A, B, C>) {
      val e = encoder.beginStructure(descriptor)
      e.encodeSerializableElement(serializerA.descriptor, 0, serializerA, obj.a)
      e.encodeSerializableElement(serializerB.descriptor, 1, serializerB, obj.b)
      e.encodeSerializableElement(serializerC.descriptor, 2, serializerC, obj.c)
      e.endStructure(descriptor)
   }

   override fun deserialize(decoder: Decoder): Tuple3<A, B, C> {
      val d = decoder.beginStructure(descriptor)
      d.decodeElementIndex(serializerA.descriptor)
      val a = d.decodeSerializableElement(descriptor, 0, serializerA)
      d.decodeElementIndex(serializerB.descriptor)
      val b = d.decodeSerializableElement(descriptor, 1, serializerB)
      d.decodeElementIndex(serializerC.descriptor)
      val c = d.decodeSerializableElement(descriptor, 2, serializerC)
      d.endStructure(descriptor)
      return Tuple3(a, b, c)
   }
}