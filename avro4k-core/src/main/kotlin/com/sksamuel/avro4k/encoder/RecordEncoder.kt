package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.AnnotationExtractor
import com.sksamuel.avro4k.ListRecord
import com.sksamuel.avro4k.Record
import com.sksamuel.avro4k.schema.extractNonNull
import kotlinx.serialization.*
import kotlinx.serialization.builtins.AbstractEncoder
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

interface StructureEncoder : FieldEncoder {

   override fun beginStructure(descriptor: SerialDescriptor, vararg typeSerializers: KSerializer<*>): CompositeEncoder {
      return when (descriptor.kind) {
         StructureKind.LIST -> {
            when (descriptor.getElementDescriptor(0).kind) {
               PrimitiveKind.BYTE -> ByteArrayEncoder(fieldSchema(), context) { addValue(it) }
               else -> ListEncoder(fieldSchema(), context) { addValue(it) }
            }
         }
         StructureKind.CLASS -> RecordEncoder(fieldSchema(), context, descriptor) { addValue(it) }
         StructureKind.MAP -> MapEncoder(fieldSchema(), context, descriptor) { addValue(it) }
         PolymorphicKind.SEALED -> SealedClassEncoder(fieldSchema(), context, descriptor) { addValue(it) }
         else -> throw SerializationException(".beginStructure was called on a non-structure type [$descriptor]")
      }
   }
}

class RecordEncoder(private val schema: Schema,
                    override val context: SerialModule,
                    private val desc: SerialDescriptor,
                    val callback: (Record) -> Unit) : AbstractEncoder(), StructureEncoder {

   private val builder = RecordBuilder(schema)
   private var currentIndex = -1

   override fun fieldSchema(): Schema {
      // if the element is nullable, then we should have a union schema which we can extract the non-null schema from
      return if (desc.getElementDescriptor(currentIndex).isNullable) {
         schema.fields[currentIndex].schema().extractNonNull()
      } else {
         schema.fields[currentIndex].schema()
      }
   }

   override fun addValue(value: Any) {
      builder.add(value)
   }

   override fun encodeString(value: String) {
      builder.add(StringToAvroValue.toValue(fieldSchema(), value))
   }

   override fun encodeValue(value: Any) {
      builder.add(value)
   }

   override fun encodeElement(descriptor: SerialDescriptor, index: Int): Boolean {
      currentIndex = index
      return true
   }

   override fun encodeByteArray(buffer: ByteBuffer) {
      builder.add(buffer)
   }

   override fun encodeFixed(fixed: GenericFixed) {
      builder.add(fixed)
   }

   override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
      builder.add(ValueToEnum.toValue(fieldSchema(), enumDescriptor, index))
   }

   override fun beginStructure(descriptor: SerialDescriptor, vararg typeSerializers: KSerializer<*>): CompositeEncoder {
      // if we have a value type, then we don't want to begin a new structure
      return if (AnnotationExtractor(descriptor.annotations).valueType())
         this
      else
         super<StructureEncoder>.beginStructure(descriptor, *typeSerializers)
   }

   override fun endStructure(descriptor: SerialDescriptor) {
      callback(builder.record())
   }

   override fun encodeNull() {
      builder.add(null)
   }
}

class RecordBuilder(private val schema: Schema) {

   private val values = arrayListOf<Any?>()

   fun add(value: Any?) = values.add(value)

   fun record(): Record = ListRecord(schema, values)
}