package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.NamingStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind

@ExperimentalSerializationApi
class AnnotationExtractor(private val annotations: List<Annotation>) {
    fun fixed(): Int? = annotations.filterIsInstance<AvroFixed>().firstOrNull()?.size
    fun scalePrecision(): Pair<Int, Int>? = annotations.filterIsInstance<ScalePrecision>().firstOrNull()?.let { it.scale to it.precision }
    fun namespace(): String? = annotations.filterIsInstance<AvroNamespace>().firstOrNull()?.value
    fun name(): String? = annotations.filterIsInstance<AvroName>().firstOrNull()?.value
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
internal fun SerialDescriptor.getAvroFullName(namingStrategy: NamingStrategy): String {
    val namespace = annotations.firstOrNull { it is AvroNamespace }?.let { (it as AvroNamespace).value }
    val name = annotations.firstOrNull { it is AvroName }?.let { namingStrategy.to((it as AvroName).value) }
    return if (namespace != null) {
        if (name != null) {
            "$namespace$name"
        } else {
            val serialOnlyName = namingStrategy.to(serialName.substringAfterLast('.').let { if (isNullable) it.removeSuffix('?') else it })
            "$namespace$serialOnlyName"
        }
    } else {
        if (name != null) {
            "$name"
        } else {
            val dotSeparatorIndex = serialName.lastIndexOf('.')
            "${serialName.substring(0, dotSeparatorIndex)}.${namingStrategy.to(serialName.substring(dotSeparatorIndex + 1).let { if (isNullable) it.removeSuffix('?') else it })}"
        }
    }
}

@ExperimentalSerializationApi
fun String.removeSuffix(char: Char): String =
        if (endsWith(char)) substring(0, length - 1) else this

@ExperimentalSerializationApi
internal fun SerialDescriptor.getElementAvroName(namingStrategy: NamingStrategy, index: Int): String {
    val name = getElementAnnotations(index).firstOrNull { it is AvroName }?.let { (it as AvroName).value }
    return if (name != null) {
        "$name"
    } else {
        namingStrategy.to(getElementName(index))
    }
}

@ExperimentalSerializationApi
fun SerialDescriptor.isListOfBytes() =
        kind == StructureKind.LIST && getElementDescriptor(0).kind == PrimitiveKind.BYTE
