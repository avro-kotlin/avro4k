package com.github.avrokotlin.benchmark

import com.github.avrokotlin.benchmark.complex.Avro4kBenchmark
import com.github.avrokotlin.benchmark.lists.ApacheAvroReflectListsBenchmark
import com.github.avrokotlin.benchmark.simple.JacksonAvroSimpleBenchmark

internal object ManualProfilingWrite {
    @JvmStatic
    fun main(vararg args: String) {
        JacksonAvroSimpleBenchmark().apply {
            initTestData()
            for (i in 0 until 1_000_000) {
                if (i % 1_000 == 0) println("Iteration $i")
                write()
                read()
            }
        }
    }
}
internal object ManualProfilingRead {
    @JvmStatic
    fun main(vararg args: String) {
        Avro4kBenchmark().apply {
            initTestData()
            for (i in 0 until 1_000_000) {
                if (i % 1_000 == 0) println("Iteration $i")
                read()
            }
        }
    }
}

internal object ManualProfilingListRead {
    @JvmStatic
    fun main(vararg args: String) {
        ApacheAvroReflectListsBenchmark().apply {
            initTestData()
            for (i in 0 until 1_000) {
                if (i % 100 == 0) println("Iteration $i")
                read()
            }
        }
    }
}