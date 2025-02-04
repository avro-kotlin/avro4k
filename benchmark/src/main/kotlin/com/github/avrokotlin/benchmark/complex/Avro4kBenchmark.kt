package com.github.avrokotlin.benchmark.complex

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.encodeToStream
import com.github.avrokotlin.benchmark.internal.Clients
import kotlinx.benchmark.Benchmark
import kotlinx.serialization.ExperimentalSerializationApi
import java.io.OutputStream

internal class Avro4kBenchmark : SerializationBenchmark() {
    lateinit var data: ByteArray

    override fun setup() {
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, clients)
    }

    @Benchmark
    fun read() {
        Avro.decodeFromByteArray<Clients>(schema, data)
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Benchmark
    fun write() {
        Avro.encodeToStream(schema, clients, OutputStream.nullOutputStream())
    }
}