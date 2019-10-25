package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.AnnotationExtractor
import com.sksamuel.avro4k.AvroProp
import com.sksamuel.avro4k.RecordNaming
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

class ClassSchemaFor(private val context: SerialModule,
                     private val descriptor: SerialDescriptor) : SchemaFor {

   override fun schema(namingStrategy: NamingStrategy): Schema {

      val annos = AnnotationExtractor(descriptor.getEntityAnnotations())
      val naming = RecordNaming(descriptor)

      val fields = (0 until descriptor.elementsCount)
         .map { index -> buildField(index, naming.namespace()) }

      val record = Schema.createRecord(naming.name(), annos.doc(), naming.namespace(), false)
      record.fields = fields
      annos.aliases().forEach { record.addAlias(it) }
      annos.props().forEach { (k, v) -> record.addProp(k, v) }
      return record
   }

   private fun buildField(index: Int, containingNamespace: String): Schema.Field {

      val fieldDescriptor = descriptor.getElementDescriptor(index)
      val annos = AnnotationExtractor(descriptor.getElementAnnotations(index))
      val naming = RecordNaming(descriptor, index)
      val schema = schemaFor(context,
         fieldDescriptor,
         descriptor.getElementAnnotations(index))
         .schema(DefaultNamingStrategy)

      // if we have annotated the field @AvroFixed then we override the type and change it to a Fixed schema
      // if someone puts @AvroFixed on a complex type, it makes no sense, but that's their cross to bear
      // in addition, someone could annotate the target type, so we need to check into that too
      val (size, name) = when (val a = annos.fixed()) {
         null -> {
            val fieldAnnos = AnnotationExtractor(fieldDescriptor.getEntityAnnotations())
            val fieldNaming = RecordNaming(fieldDescriptor)
            when (val b = fieldAnnos.fixed()) {
               null -> 0 to naming.name()
               else -> b to fieldNaming.name()
            }
         }
         else -> a to naming.name()
      }
      val schemaOrFixed = when (size) {
         0 -> schema
         else ->
            SchemaBuilder.fixed(name)
               .doc(annos.doc())
               .namespace(annos.namespace() ?: containingNamespace)
               .size(size)
      }

      // the field can override the containingNamespace if the Namespace annotation is present on the field
      // we may have annotated our field with @AvroNamespace so this containingNamespace should be applied
      // to any schemas we have generated for this field
      val schemaWithResolvedNamespace = when (val ns = annos.namespace()) {
         null -> schemaOrFixed
         else -> schemaOrFixed.overrideNamespace(ns)
      }

      val field = Schema.Field(naming.name(), schemaWithResolvedNamespace, annos.doc(), null)
      val props = this.descriptor.getElementAnnotations(index).filterIsInstance<AvroProp>()
      props.forEach { field.addProp(it.key, it.value) }
      annos.aliases().forEach { field.addAlias(it) }

      return field
   }
}