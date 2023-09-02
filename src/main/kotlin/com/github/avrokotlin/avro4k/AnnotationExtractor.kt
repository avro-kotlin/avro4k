package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.DefaultNamingStrategy
import com.github.avrokotlin.avro4k.schema.NamingStrategy
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
class AnnotationExtractor(private val annotations: List<Annotation>) {
    fun fixed(): Int? = annotations.filterIsInstance<AvroFixed>().firstOrNull()?.size
    fun scalePrecision(): Pair<Int, Int>? = annotations.filterIsInstance<ScalePrecision>().firstOrNull()?.let { it.scale to it.precision }
    fun doc(): String? = annotations.filterIsInstance<AvroDoc>().firstOrNull()?.value
    fun aliases(): Array<String> {
        val alias = annotations.firstOrNull { it is AvroAlias }?.let { it as AvroAlias }?.value ?: emptyArray()
        val aliases = annotations.firstOrNull { it is AvroAliases }?.let { it as AvroAliases }?.value ?: emptyArray()
        return aliases + alias
    }

    fun props(): List<Pair<String, String>> = annotations.filterIsInstance<AvroProp>().map { it.key to it.value }
    fun jsonProps(): List<Pair<String, String>> = annotations.filterIsInstance<AvroJsonProp>().map { it.key to it.jsonValue }
    fun default(): String? = annotations.filterIsInstance<AvroDefault>().firstOrNull()?.value
    fun enumDefault(): String? = annotations.filterIsInstance<AvroEnumDefault>().firstOrNull()?.value
}

@ExperimentalSerializationApi
internal fun SerialDescriptor.getAvroName(namingStrategy: NamingStrategy): RecordNaming {
    fun String.normalizeNullableSuffix() =
        if (isNullable) this.removeSuffix('?') else this

    return getAvroName(serialName.normalizeNullableSuffix(), annotations, namingStrategy)
}

@ExperimentalSerializationApi
internal fun SerialDescriptor.getElementAvroName(namingStrategy: NamingStrategy, index: Int): RecordNaming {
    return getAvroName(getElementName(index), getElementAnnotations(index), namingStrategy)
}

@ExperimentalSerializationApi
private fun getAvroName(serialName: String, annotations: List<Annotation>, namingStrategy: NamingStrategy): RecordNaming {
    val dotIndex by lazy { serialName.lastIndexOf('.').takeIf { it >= 0 } }
    val namespace = annotations.firstIsInstanceOrNull<AvroNamespace>()?.value
        ?: dotIndex?.let { serialName.substring(0, it) }
    val name = annotations.firstIsInstanceOrNull<AvroName>()?.value
        ?: dotIndex?.let { serialName.substring(it + 1) }
        ?: serialName

    return RecordNaming(
        name = namingStrategy.to(name),
        namespace = namespace?.takeIf { it.isNotEmpty() },
    )
}

internal fun String.removeSuffix(char: Char): String =
    if (endsWith(char)) substring(0, length - 1) else this

internal inline fun <reified T> Iterable<*>.firstIsInstanceOrNull(): T? {
    return firstNotNullOfOrNull { it as? T }
}

internal fun String.substringBeforeLastOrNull(delimiter: Char): String? {
    val index = lastIndexOf(delimiter)
    return if (index == -1) null else substring(0, index)
}

@ExperimentalSerializationApi
fun SerialDescriptor.isListOfBytes() =
    kind == StructureKind.LIST && getElementDescriptor(0).kind == PrimitiveKind.BYTE

/**
 * Coming from [org.apache.avro.generic.GenericData.getSchemaName]
 */
@ExperimentalSerializationApi
fun SerialDescriptor.getSchemaNameForUnion(): RecordNaming {
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
        SerialKind.ENUM -> getAvroName(DefaultNamingStrategy)
        StructureKind.CLASS, StructureKind.OBJECT -> getAvroName(DefaultNamingStrategy)
        SerialKind.CONTEXTUAL, is PolymorphicKind -> throw SerializationException("getSchemaName should be called on an already resolved descriptor (not a contextual or polymorphic). Actual descriptor: $this")
    }
}
