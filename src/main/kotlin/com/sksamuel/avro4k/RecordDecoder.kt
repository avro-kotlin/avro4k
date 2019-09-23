package com.sksamuel.avro4k

import kotlinx.serialization.CompositeDecoder
import kotlinx.serialization.ElementValueDecoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.NamedValueDecoder
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.StructureKind
import org.apache.avro.generic.GenericData

class RecordDecoder(val record: GenericData.Record) : NamedValueDecoder() {

  override fun decodeTaggedDouble(tag: String): Double {
    return record.get(tag) as Double
  }

  override fun decodeTaggedLong(tag: String): Long {
    return record.get(tag) as Long
  }

  override fun decodeTaggedFloat(tag: String): Float {
    return record.get(tag) as Float
  }

  override fun decodeTaggedBoolean(tag: String): Boolean {
    return record.get(tag) as Boolean
  }

  override fun decodeTaggedInt(tag: String): Int {
    return record.get(tag) as Int
  }

  override fun decodeTaggedString(tag: String): String {
    println(tag)
    return record.get(tag) as String
  }

  var index = 0

  override fun decodeElementIndex(desc: SerialDescriptor): Int {
    println("decodeElementIndex $desc $index")
    if (index == desc.elementsCount) return CompositeDecoder.READ_DONE
    val k = index
    index++
    return k
  }

  private val lists = mutableListOf<Array<Any>>()

  override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeDecoder {
    println("beginStructure $desc")
    return when (desc.kind as StructureKind) {
      StructureKind.CLASS -> this
      StructureKind.LIST -> {
        when (val data = record.get(currentTag)) {
          is List<*> -> ListDecoder(data)
          is Array<*> -> ListDecoder(data.asList())
          else -> this
        }
      }
      StructureKind.MAP -> this
    }
  }

  override fun endStructure(desc: SerialDescriptor) {
    println("endStructure $desc")
    super.endStructure(desc)
  }

  override fun decodeCollectionSize(desc: SerialDescriptor): Int {
    return super.decodeCollectionSize(desc)
  }
}

class ListDecoder(private val array: List<Any?>) : ElementValueDecoder() {

  private var index = 0

  override fun decodeBoolean(): Boolean {
    return array[index++] as Boolean
  }

  override fun decodeLong(): Long {
    return array[index++] as Long
  }

  override fun decodeString(): String {
    return array[index++] as String
  }

  override fun decodeDouble(): Double {
    return array[index++] as Double
  }

  override fun decodeFloat(): Float {
    return array[index++] as Float
  }

  override fun decodeCollectionSize(desc: SerialDescriptor): Int = array.size
}