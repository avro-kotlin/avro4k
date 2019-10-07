package com.sksamuel.avro4k.schema

import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerialKind
import kotlinx.serialization.SerializationException
import org.apache.avro.Schema
import kotlin.reflect.KClass
import kotlin.reflect.jvm.jvmName

abstract class AvroDescriptor(override val name: String,
                              override val kind: SerialKind) : SerialDescriptor {

   constructor(type: KClass<*>, kind: SerialKind) : this(type.jvmName, kind)

   abstract fun schema(annos: List<Annotation>): Schema

   override fun getElementIndex(name: String): Int = -1
   override fun getElementName(index: Int): String = throw SerializationException("AvroDescriptor has no child elements")
}