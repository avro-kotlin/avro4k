package com.sksamuel.avro4k.schema

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import kotlin.reflect.KClass
import kotlin.reflect.jvm.jvmName

@OptIn(ExperimentalSerializationApi::class)
abstract class AvroDescriptor(override val serialName: String,
                              override val kind: SerialKind
) : SerialDescriptor {

   constructor(type: KClass<*>, kind: SerialKind) : this(type.jvmName, kind)

   abstract fun schema(annos: List<Annotation>,
                       serializersModule: SerializersModule,
                       namingStrategy: NamingStrategy): Schema

   override val elementsCount: Int
      get() = 0

   private fun failNoChildDescriptors() : Nothing = throw SerializationException("AvroDescriptor has no child elements")
   override fun isElementOptional(index: Int): Boolean  = false
   override fun getElementDescriptor(index: Int): SerialDescriptor = failNoChildDescriptors()
   override fun getElementAnnotations(index: Int): List<Annotation>  = emptyList()
   override fun getElementIndex(name: String): Int = -1
   override fun getElementName(index: Int): String = failNoChildDescriptors()
}