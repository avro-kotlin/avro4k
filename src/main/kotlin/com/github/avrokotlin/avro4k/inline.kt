package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

@ExperimentalSerializationApi
val SerialDescriptor.isInline: Boolean
   get() = kotlin.runCatching { Class.forName(serialName).kotlin.isInline }.getOrDefault(false)

val KClass<*>.isInline: Boolean
   get() = !isData &&
      primaryConstructor?.parameters?.size == 1 &&
      java.declaredMethods.any { it.name == "box-impl" }