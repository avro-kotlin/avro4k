package com.github.avrokotlin.benchmark.simple

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.encodeToStream
import com.github.avrokotlin.benchmark.internal.SimpleDatasClass
import kotlinx.benchmark.Benchmark
import kotlinx.serialization.ExperimentalSerializationApi
import java.io.OutputStream

internal class Avro4kSimpleBenchmark : SerializationSimpleBenchmark() {
    lateinit var data: ByteArray

    override fun setup() {
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, clients)
    }

    @Benchmark
    fun read() {
        Avro.decodeFromByteArray<SimpleDatasClass>(schema, data)
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Benchmark
    fun write() {
        Avro.encodeToStream(schema, clients, OutputStream.nullOutputStream())
    }
}