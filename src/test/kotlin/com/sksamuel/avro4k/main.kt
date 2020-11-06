package com.sksamuel.avro4k

import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer

fun main() {
   println(Avro.default.schema(Task.serializer()))

   Avro.default.encodeToByteArray(Task.serializer(), Task(status = StatusOn))
}

@Serializable
data class Task(
   var status: Status
)

@Serializable
abstract class Status

@Serializable
object StatusOn : Status() {
   override fun equals(other: Any?) = javaClass == other?.javaClass
}

@Serializable
data class StatusOff(
   val result: String
) : Status()


