package com.github.avrokotlin.benchmark

import com.github.avrokotlin.benchmark.gen.ClientsGenerator
import kotlinx.benchmark.*
import java.util.concurrent.TimeUnit


@State(Scope.Benchmark)
@Warmup(iterations = 3)
@BenchmarkMode(Mode.Throughput)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
internal abstract class SerializationBenchmark {
    lateinit var clients: Clients

    @Setup
    fun initTestData(){
        clients = Clients()
        ClientsGenerator.populate(clients, 1000)
        prepareBinaryData()
    }

    abstract fun prepareBinaryData()
}