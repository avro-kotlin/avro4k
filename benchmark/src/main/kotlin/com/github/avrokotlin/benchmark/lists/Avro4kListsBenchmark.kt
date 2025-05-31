package com.github.avrokotlin.benchmark.lists

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.encodeToSink
import com.github.avrokotlin.benchmark.internal.ListWrapperDatasClass
import kotlinx.benchmark.Benchmark
import kotlinx.io.asSink
import kotlinx.io.buffered
import kotlinx.serialization.ExperimentalSerializationApi
import java.io.OutputStream

internal class Avro4kListsBenchmark : SerializationListsBenchmark() {
    lateinit var data: ByteArray

    override fun setup() {
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, lists)
    }

    @Benchmark
    fun read() {
        Avro.decodeFromByteArray<ListWrapperDatasClass>(schema, data)
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Benchmark
    fun write() {
        Avro.encodeToSink(schema, lists, OutputStream.nullOutputStream().asSink().buffered())
    }
}