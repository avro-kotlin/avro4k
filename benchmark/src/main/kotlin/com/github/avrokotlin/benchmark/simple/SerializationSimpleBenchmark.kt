package com.github.avrokotlin.benchmark.simple

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.benchmark.internal.RandomUtils
import com.github.avrokotlin.benchmark.internal.SimpleDatasClass
import kotlinx.benchmark.*
import java.util.concurrent.TimeUnit


@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
internal abstract class SerializationSimpleBenchmark {
    lateinit var clients: SimpleDatasClass
    val schema = Avro.schema<SimpleDatasClass>()

    @Setup
    fun initTestData() {
        setup()
        clients = SimpleDatasClass.create(25, RandomUtils())
        prepareBinaryData()
    }

    abstract fun setup()

    abstract fun prepareBinaryData()
}