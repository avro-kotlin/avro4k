package com.github.avrokotlin.benchmark.lists

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.encodeToStream
import com.github.avrokotlin.benchmark.internal.Clients
import com.github.avrokotlin.benchmark.internal.ListWrapperDatasClass
import kotlinx.benchmark.Benchmark
import kotlinx.serialization.ExperimentalSerializationApi
import java.io.OutputStream

internal class Avro4kListsBenchmark : SerializationListsBenchmark() {
    lateinit var data: ByteArray
    var writeMode = false

    override fun setup() {
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, lists)
    }

    @Benchmark
    fun read() {
        if (writeMode) writeMode = false
        Avro.decodeFromByteArray<ListWrapperDatasClass>(schema, data)
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Benchmark
    fun write() {
        if (!writeMode) writeMode = true
        Avro.encodeToStream(schema, lists, OutputStream.nullOutputStream())
    }
}