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
    var writeMode = false

    override fun setup() {
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, clients)
    }

    @Benchmark
    fun read() {
        if (writeMode) writeMode = false
        Avro.decodeFromByteArray<Clients>(schema, data)
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Benchmark
    fun write() {
        if (!writeMode) writeMode = true
        Avro.encodeToStream(schema, clients, OutputStream.nullOutputStream())
    }
}