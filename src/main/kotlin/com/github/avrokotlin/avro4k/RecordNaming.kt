package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi


@ExperimentalSerializationApi
data class RecordNaming(
    val name: String,
    val namespace: String?,
) {
    val fullName: String
       get() = if (namespace != null) "$namespace.$name" else name

    companion object {
         fun onlyName(
                name: String,
        ) = RecordNaming(name, namespace = null)
    }

   override fun toString() = fullName
}
