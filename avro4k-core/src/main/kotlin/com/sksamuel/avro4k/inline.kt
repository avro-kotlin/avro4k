package com.sksamuel.avro4k

import kotlinx.serialization.SerialDescriptor
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

val SerialDescriptor.isInline: Boolean
   get() = kotlin.runCatching { Class.forName(name).kotlin.isInline }.getOrDefault(false)

val KClass<*>.isInline: Boolean
   get() = !isData &&
      primaryConstructor?.parameters?.size == 1 &&
      java.declaredMethods.any { it.name == "box-impl" }