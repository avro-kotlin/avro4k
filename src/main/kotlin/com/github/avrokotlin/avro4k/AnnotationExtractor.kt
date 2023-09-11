package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import org.apache.avro.Schema
import java.math.BigDecimal

@ExperimentalSerializationApi
internal class AnnotationExtractor(private val annotations: List<Annotation>) {
    fun fixed(): Int? = annotations.filterIsInstance<AvroFixed>().firstOrNull()?.size
    fun scalePrecision(): Pair<Int, Int>? =
        annotations.filterIsInstance<ScalePrecision>().firstOrNull()?.let { it.scale to it.precision }

    fun doc(): String? = annotations.filterIsInstance<AvroDoc>().firstOrNull()?.value
    fun aliases(): Array<String> {
        val alias = annotations.firstOrNull { it is AvroAlias }?.let { it as AvroAlias }?.value ?: emptyArray()
        val aliases = annotations.firstOrNull { it is AvroAliases }?.let { it as AvroAliases }?.value ?: emptyArray()
        return aliases + alias
    }

    fun props(): List<Pair<String, String>> = annotations.filterIsInstance<AvroProp>().map { it.key to it.value }
    fun jsonProps(): List<Pair<String, String>> =
        annotations.filterIsInstance<AvroJsonProp>().map { it.key to it.jsonValue }

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
        return RecordNaming.onlyName(Schema.Type.BYTES.getName())
    }
    // startsWith because if type nullable, then it ends with "?"
    if (isNullable && serialName.startsWith(BigDecimal::class.qualifiedName!!) || serialName == BigDecimal::class.qualifiedName) // todo change BigDecimalSerializer kind to LIST of BYTE
        return RecordNaming.onlyName(Schema.Type.BYTES.getName())
    return when (kind) {
        PrimitiveKind.BOOLEAN -> RecordNaming.onlyName(Schema.Type.BOOLEAN.getName())
        PrimitiveKind.DOUBLE -> RecordNaming.onlyName(Schema.Type.DOUBLE.getName())
        PrimitiveKind.FLOAT -> RecordNaming.onlyName(Schema.Type.FLOAT.getName())
        PrimitiveKind.BYTE,
        PrimitiveKind.SHORT,
        PrimitiveKind.CHAR,
        PrimitiveKind.INT -> RecordNaming.onlyName(Schema.Type.INT.getName())

        PrimitiveKind.LONG -> RecordNaming.onlyName(Schema.Type.LONG.getName())
        PrimitiveKind.STRING -> RecordNaming.onlyName(Schema.Type.STRING.getName())
        StructureKind.LIST -> RecordNaming.onlyName(Schema.Type.ARRAY.getName())
        StructureKind.MAP -> RecordNaming.onlyName(Schema.Type.MAP.getName())
        SerialKind.ENUM -> nameResolver.resolveTypeName(this)
        StructureKind.CLASS, StructureKind.OBJECT -> nameResolver.resolveTypeName(this)
        SerialKind.CONTEXTUAL, is PolymorphicKind -> throw SerializationException("getSchemaNameForUnion should be called on an already resolved descriptor (not a contextual or polymorphic). Actual descriptor: $this")
    }
}
