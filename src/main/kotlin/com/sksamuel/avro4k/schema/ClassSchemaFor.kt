package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.*
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.JsonProperties
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

@ExperimentalSerializationApi
class ClassSchemaFor(private val descriptor: SerialDescriptor,
                     private val namingStrategy: NamingStrategy,
                     private val serializersModule: SerializersModule
) : SchemaFor {

   private val entityAnnotations = AnnotationExtractor(descriptor.annotations)
   private val naming = RecordNaming(descriptor)
   private val json by lazy {
      Json{
         serializersModule = this@ClassSchemaFor.serializersModule
      }
   }

   override fun schema(): Schema {
      // if the class is annotated with @AvroValueType then we need to encode the single field
      // of that class directly.
      return when (entityAnnotations.valueType()) {
         true -> valueTypeSchema()
         false -> dataClassSchema()
      }
   }

   private fun valueTypeSchema(): Schema {
      require(descriptor.elementsCount == 1) { "A value type must only have a single field" }
      return buildField(0).schema()
   }

   private fun dataClassSchema(): Schema {

      val fields = (0 until descriptor.elementsCount)
         .map { index -> buildField(index) }

      val record = Schema.createRecord(naming.name(), entityAnnotations.doc(), naming.namespace(), false)
      record.fields = fields
      entityAnnotations.aliases().forEach { record.addAlias(it) }
      entityAnnotations.props().forEach { (k, v) -> record.addProp(k, v) }
      return record
   }

   private fun buildField(index: Int): Schema.Field {

      val fieldDescriptor = descriptor.getElementDescriptor(index)
      val annos = AnnotationExtractor(descriptor.getElementAnnotations(index))
      val fieldNaming = RecordNaming(descriptor, index)
      val schema = schemaFor(serializersModule, fieldDescriptor, descriptor.getElementAnnotations(index), namingStrategy)
         .schema()

      // if we have annotated the field @AvroFixed then we override the type and change it to a Fixed schema
      // if someone puts @AvroFixed on a complex type, it makes no sense, but that's their cross to bear
      // in addition, someone could annotate the target type, so we need to check into that too
      val (size, name) = when (val a = annos.fixed()) {
         null -> {
            val fieldAnnos = AnnotationExtractor(fieldDescriptor.annotations)
            val n = RecordNaming(fieldDescriptor)
            when (val b = fieldAnnos.fixed()) {
               null -> 0 to n.name()
               else -> b to n.name()
            }
         }
         else -> a to fieldNaming.name()
      }

      val schemaOrFixed = when (size) {
         0 -> schema
         else ->
            SchemaBuilder.fixed(name)
               .doc(annos.doc())
               .namespace(annos.namespace() ?: naming.namespace())
               .size(size)
      }

      // the field can override the containingNamespace if the Namespace annotation is present on the field
      // we may have annotated our field with @AvroNamespace so this containingNamespace should be applied
      // to any schemas we have generated for this field
      val schemaWithResolvedNamespace = when (val ns = annos.namespace()) {
         null -> schemaOrFixed
         else -> schemaOrFixed.overrideNamespace(ns)
      }


      val default: Any? = annos.default()?.let {
         if (it == Avro.NULL) {
            Schema.Field.NULL_DEFAULT_VALUE
         } else {
            when (fieldDescriptor.kind) {
               PrimitiveKind.INT -> it.toInt()
               PrimitiveKind.LONG -> it.toLong()
               PrimitiveKind.FLOAT -> it.toFloat()
               PrimitiveKind.BOOLEAN -> it.toBoolean()
               PrimitiveKind.BYTE -> it.toByte()
               PrimitiveKind.SHORT -> it.toShort()
               PrimitiveKind.STRING -> it
               StructureKind.LIST ->
                  decodeJsonDefaultAsList(fieldDescriptor
                  , it)

               else -> throw IllegalArgumentException("Cannot use a default value for type ${fieldDescriptor.kind}")
            }
         }
      }

      val field = Schema.Field(fieldNaming.name(), schemaWithResolvedNamespace, annos.doc(), default)
      val props = this.descriptor.getElementAnnotations(index).filterIsInstance<AvroProp>()
      props.forEach { field.addProp(it.key, it.value) }
      annos.aliases().forEach { field.addAlias(it) }

      return field
   }

   private fun  decodeJsonDefaultAsList(listFieldDescriptor: SerialDescriptor, jsonString: String): List<Any> = try {
      // the list entries will be parsed according to their kind
      val decodedValue = json.decodeFromString(listFieldDescriptor.serializer(), jsonString)
      (decodedValue as? List<*>)?.map { it?:JsonProperties.NULL_VALUE } ?: error("Serializer of an array field descriptor did not return a List in its deserialized form.")
   } catch (se: SerializationException) {
      throw IllegalArgumentException("Cannot use default value $jsonString. ${se.message}",se)
   }


}

