package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.*
import org.apache.avro.Schema
import java.math.BigDecimal

@ExperimentalSerializationApi
class AnnotationExtractor(private val annotations: List<Annotation>) {

   companion object {
      fun entity(descriptor: SerialDescriptor) = AnnotationExtractor(
         descriptor.annotations)

      operator fun invoke(descriptor: SerialDescriptor, index: Int): AnnotationExtractor =
         AnnotationExtractor(descriptor.getElementAnnotations(index))
   }

   fun fixed(): Int? = annotations.filterIsInstance<AvroFixed>().firstOrNull()?.size
   fun scalePrecision(): Pair<Int, Int>? = annotations.filterIsInstance<ScalePrecision>().firstOrNull()?.let { it.scale to it.precision }
   fun namespace(): String? = annotations.filterIsInstance<AvroNamespace>().firstOrNull()?.value
   fun name(): String? = annotations.filterIsInstance<AvroName>().firstOrNull()?.value
   fun valueType(): Boolean = annotations.filterIsInstance<AvroInline>().isNotEmpty()
   fun doc(): String? = annotations.filterIsInstance<AvroDoc>().firstOrNull()?.value
   fun aliases(): List<String> = (annotations.firstNotNullOfOrNull { it as? AvroAlias }?.value ?: emptyArray()).asList() + (annotations.firstNotNullOfOrNull {it as? AvroAliases}?.value ?: emptyArray())
   fun props(): List<Pair<String, String>> = annotations.filterIsInstance<AvroProp>().map { it.key to it.value }
   fun jsonProps(): List<Pair<String, String>> = annotations.filterIsInstance<AvroJsonProp>().map { it.key to it.jsonValue }
   fun default(): String? = annotations.filterIsInstance<AvroDefault>().firstOrNull()?.value
   fun enumDefault(): String? = annotations.filterIsInstance<AvroEnumDefault>().firstOrNull()?.value
}
internal inline fun <reified T> Iterable<*>.firstIsInstanceOrNull(): T? {
   return firstNotNullOfOrNull { it as? T }
}
@ExperimentalSerializationApi
internal fun SerialDescriptor.isListOfBytes() =
   kind == StructureKind.LIST && getElementDescriptor(0).kind == PrimitiveKind.BYTE
/**
 * Coming from [org.apache.avro.generic.GenericData.getSchemaName]
 */
@ExperimentalSerializationApi
internal fun SerialDescriptor.getSchemaNameForUnion(nameResolver: AvroNameResolver): RecordNaming {
   if (isListOfBytes()) {
      return RecordNaming(Schema.Type.BYTES.getName(), null)
   }
   // startsWith because if type nullable, then it ends with "?"
   if (isNullable && serialName.startsWith(BigDecimal::class.qualifiedName!!) || serialName == BigDecimal::class.qualifiedName) // todo change BigDecimalSerializer kind to LIST of BYTE
      return RecordNaming(Schema.Type.BYTES.getName(), null)
   return when (kind) {
      PrimitiveKind.BOOLEAN -> RecordNaming(Schema.Type.BOOLEAN.getName(), null)
      PrimitiveKind.DOUBLE -> RecordNaming(Schema.Type.DOUBLE.getName(), null)
      PrimitiveKind.FLOAT -> RecordNaming(Schema.Type.FLOAT.getName(), null)
      PrimitiveKind.BYTE,
      PrimitiveKind.SHORT,
      PrimitiveKind.CHAR,
      PrimitiveKind.INT -> RecordNaming(Schema.Type.INT.getName(), null)

      PrimitiveKind.LONG -> RecordNaming(Schema.Type.LONG.getName(), null)
      PrimitiveKind.STRING -> RecordNaming(Schema.Type.STRING.getName(), null)
      StructureKind.LIST -> RecordNaming(Schema.Type.ARRAY.getName(), null)
      StructureKind.MAP -> RecordNaming(Schema.Type.MAP.getName(), null)
      SerialKind.ENUM -> nameResolver.resolveTypeName(this)
      StructureKind.CLASS, StructureKind.OBJECT -> nameResolver.resolveTypeName(this)
      SerialKind.CONTEXTUAL, is PolymorphicKind -> throw SerializationException("getSchemaNameForUnion should be called on an already resolved descriptor (not a contextual or polymorphic). Actual descriptor: $this")
   }
}