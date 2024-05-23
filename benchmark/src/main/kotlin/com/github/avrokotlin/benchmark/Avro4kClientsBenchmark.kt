package com.github.avrokotlin.benchmark

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.encodeToByteArray
import kotlinx.benchmark.Benchmark

internal object Avro4kClientsStaticReadBenchmark {
    @JvmStatic
    fun main(vararg args: String) {
        Avro4kClientsBenchmark().apply {
            initTestData()
            for (i in 0 until 1000000) {
                if (i % 100000 == 0) println("Iteration $i")
                read()
            }
        }
    }
}

internal object Avro4kClientsStaticWriteBenchmark {
    @JvmStatic
    fun main(vararg args: String) {
        Avro4kClientsBenchmark().apply {
            initTestData()
            for (i in 0 until 1000000) {
                if (i % 100000 == 0) println("Iteration $i")
                write()
            }
        }
    }
}

internal class Avro4kClientsBenchmark : SerializationBenchmark() {
    lateinit var data: ByteArray
    var writeMode = false

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(clients)
    }

    @Benchmark
    fun read() {
        if (writeMode) writeMode = false
        Avro.decodeFromByteArray<Clients>(data)
    }

    @Benchmark
    fun write() {
        if (!writeMode) writeMode = true
        Avro.encodeToByteArray(clients)
    }
}