package com.github.avrokotlin.benchmark.internal

import kotlinx.serialization.Serializable

@Serializable
internal data class ListWrapperDataClass(val index: Int, val entries: List<StatsEntry>) {
    companion object {
        fun create(size: Int, entriesSize: Int, random: RandomUtils = RandomUtils()) =
            ListWrapperDataClass(random.nextInt(), List(size) { StatsEntry.create(entriesSize) })
    }
}

@Serializable
internal data class StatsEntry(val elementId: ElementId, val values: List<StatValue>) {
    companion object {
        fun create(entriesSize: Int, random: RandomUtils = RandomUtils()) =
            StatsEntry(random.nextLong(), List(entriesSize) { random.nextLong() })
    }
}

typealias ElementId = Long
typealias StatValue = Long

@Serializable
internal data class ListWrapperDatasClass(
    val data: List<ListWrapperDataClass>
) {
    companion object {
        fun create(size: Int, wrapperSize: Int, entriesSize: Int = 100) =
            ListWrapperDatasClass(data = List(size) { ListWrapperDataClass.create(wrapperSize, entriesSize) }
            )
    }
}