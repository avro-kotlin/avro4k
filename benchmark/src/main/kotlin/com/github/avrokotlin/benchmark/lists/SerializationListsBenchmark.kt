package com.github.avrokotlin.benchmark.lists

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.benchmark.internal.Clients
import com.github.avrokotlin.benchmark.internal.ClientsGenerator
import com.github.avrokotlin.benchmark.internal.ListWrapperDataClass
import com.github.avrokotlin.benchmark.internal.ListWrapperDatasClass
import kotlinx.benchmark.*
import java.util.concurrent.TimeUnit


@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
internal abstract class SerializationListsBenchmark {
    lateinit var lists: ListWrapperDatasClass
    val schema = Avro.schema<ListWrapperDatasClass>()

    @Setup
    fun initTestData() {
        setup()
        lists = ListWrapperDatasClass.create(10, 10000)
        prepareBinaryData()
    }

    abstract fun setup()

    abstract fun prepareBinaryData()
}