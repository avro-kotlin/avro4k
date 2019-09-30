package com.sksamuel.avro4k.serializers

import kotlinx.serialization.ImplicitReflectionSerializer
import kotlin.reflect.KClass

@ImplicitReflectionSerializer
class KotlinxSerializationProvider {

   fun <T : Any> serializer(type: KClass<T>): Unit? {
      throw RuntimeException(type.toString())
   }
}