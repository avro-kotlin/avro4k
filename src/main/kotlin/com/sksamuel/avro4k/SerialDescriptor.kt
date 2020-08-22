package com.sksamuel.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.*

@ExperimentalSerializationApi
fun SerialDescriptor.leavesOfSealedClasses() : List<SerialDescriptor> {
   return if (this.kind == PolymorphicKind.SEALED) {
      elementDescriptors.filter {it.kind == SerialKind.CONTEXTUAL }.flatMap { it.elementDescriptors }.flatMap { it.leavesOfSealedClasses() }
   } else {
      listOf(this)
   }
}

@ExperimentalSerializationApi
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
      StructureKind.LIST -> ListSerializer(this.elementDescriptors.single().serializer())
      else -> TODO("only implemented primitive types for now")
}
   return if(this.isNullable){
      notNullableSerializer.nullable
   }else{
      notNullableSerializer
   }

}