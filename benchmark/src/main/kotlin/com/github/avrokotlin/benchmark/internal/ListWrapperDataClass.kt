package com.github.avrokotlin.benchmark.internal

import kotlinx.serialization.Serializable

@Serializable
data class ListWrapperDataClass(val parent: SimpleDataClass, val related: List<SimpleDataClass>) {
    companion object {
        fun create(size: Int) =
            ListWrapperDataClass(SimpleDataClass.create(), List(size) { SimpleDataClass.create() })
    }
}


@Serializable
data class ListWrapperDatasClass(
    val data: List<ListWrapperDataClass>
) {
    companion object {
        fun create(size: Int, wrapperSize: Int) =
            ListWrapperDatasClass(data = List(size) { ListWrapperDataClass.create(wrapperSize) }
            )
    }
}