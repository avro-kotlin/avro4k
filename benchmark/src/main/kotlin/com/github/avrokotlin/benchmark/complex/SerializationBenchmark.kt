package com.github.avrokotlin.benchmark.complex

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.benchmark.internal.Clients
import com.github.avrokotlin.benchmark.internal.ClientsGenerator
import com.github.avrokotlin.benchmark.internal.RandomUtils
import kotlinx.benchmark.*
import java.util.concurrent.TimeUnit


@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
internal abstract class SerializationBenchmark {
    lateinit var clients: Clients
    val schema = Avro.schema<Clients>()

    @Setup
    fun initTestData() {
        setup()
        clients = ClientsGenerator.generate(15, RandomUtils())
        prepareBinaryData()
    }

    abstract fun setup()

    abstract fun prepareBinaryData()
}