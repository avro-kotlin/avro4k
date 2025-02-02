package com.github.avrokotlin.benchmark.internal

import kotlinx.serialization.Serializable

@Serializable
data class ListWrapperDataClass(val index: Int, val entries: List<StatsEntry>) {
   companion object {
      fun create(size: Int, entriesSize: Int) =
         ListWrapperDataClass(RandomUtils.nextInt(), List(size) { StatsEntry.create(entriesSize) })
   }
}

@Serializable
data class StatsEntry(val elementId: ElementId, val values: List<StatValue>) {
   companion object {
      fun create(entriesSize: Int) =
         StatsEntry(RandomUtils.nextLong(), List(entriesSize) { RandomUtils.nextLong() })
   }
}

typealias ElementId = Long
typealias StatValue = Long

@Serializable
data class ListWrapperDatasClass(
   val data: List<ListWrapperDataClass>
) {
   companion object {
      fun create(size: Int, wrapperSize: Int, entriesSize: Int = 100) =
         ListWrapperDatasClass(data = List(size) { ListWrapperDataClass.create(wrapperSize, entriesSize) }
         )
   }
}