package com.github.avrokotlin.benchmark.internal

import kotlinx.serialization.Serializable

@Serializable
data class ListWrapperDataClass(val id: Int, val list: List<Int>) {
    companion object {
        fun create(size: Int) =
            ListWrapperDataClass(RandomUtils.nextInt(), List(size) { RandomUtils.nextInt() })
    }
}


@Serializable
data class ListWrapperDatasClass(
    val data: List<ListWrapperDataClass>
) {
    companion object {
        fun create(size: Int, wrapperSize: Int) = ListWrapperDatasClass(
            data = List(size) { ListWrapperDataClass.create(wrapperSize) }
        )
    }
}