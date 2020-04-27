package com.sksamuel.avro4k

import kotlinx.serialization.*
import kotlinx.serialization.builtins.list
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer

fun SerialDescriptor.leafsOfSealedClasses() : List<SerialDescriptor> {
   return if (this.kind == PolymorphicKind.SEALED) {
      elementDescriptors().filter {it.kind == UnionKind.CONTEXTUAL }.flatMap { it.elementDescriptors() }.flatMap { it.leafsOfSealedClasses() }
   } else {
      listOf(this)
   }
}

fun SerialDescriptor.serializer() : KSerializer<out Any?> {
   val notNullableSerializer = when(kind){
      PrimitiveKind.BOOLEAN -> Boolean.serializer()
      PrimitiveKind.INT -> Int.serializer()
      PrimitiveKind.LONG -> Long.serializer()
      PrimitiveKind.FLOAT -> Float.serializer()
      PrimitiveKind.BOOLEAN -> Boolean.serializer()
      PrimitiveKind.BYTE -> Byte.serializer()
      PrimitiveKind.SHORT -> Short.serializer()
      PrimitiveKind.STRING -> String.serializer()
      StructureKind.LIST -> this.elementDescriptors().single().serializer().list
      else -> TODO("only implemented primitive types for now")
}
   return if(this.isNullable){
      notNullableSerializer.nullable
   }else{
      notNullableSerializer
   }

}