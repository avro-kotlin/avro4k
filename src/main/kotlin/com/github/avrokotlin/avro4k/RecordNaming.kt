package com.github.avrokotlin.avro4k


data class RecordNaming(
    val name: String,
    val namespace: String?,
) {
    val fullName: String by lazy { if (namespace != null) "$namespace.$name" else name }

    companion object {
        fun onlyName(
            name: String,
        ) = RecordNaming(name, namespace = null)
    }

    override fun toString() = fullName
}
