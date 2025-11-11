package com.github.avrokotlin.benchmark.complex

import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectReader
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.dataformat.avro.AvroMapper
import com.fasterxml.jackson.dataformat.avro.AvroSchema
import com.fasterxml.jackson.dataformat.avro.jsr310.AvroJavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.benchmark.internal.Clients
import kotlinx.benchmark.Benchmark
import java.io.OutputStream


internal class JacksonAvroBenchmark : SerializationBenchmark() {
    lateinit var writer: ObjectWriter
    lateinit var reader: ObjectReader

    lateinit var data: ByteArray

    override fun setup() {
        writer = Clients::class.java.createWriter()
        reader = Clients::class.java.createReader()
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, clients)
    }

    @Benchmark
    fun read() {
        reader.readValue<Clients>(data)
    }

    @Benchmark
    fun write() {
        writer.writeValue(OutputStream.nullOutputStream(), clients)
    }

    private fun <T> Class<T>.createWriter(): ObjectWriter {
        val mapper = avroMapper()

        return mapper.writer(AvroSchema(schema)).forType(this)
    }

    private fun <T> Class<T>.createReader(): ObjectReader {
        val mapper = avroMapper()

        return mapper.reader(AvroSchema(schema)).forType(this)
    }

    private fun avroMapper(): ObjectMapper = AvroMapper()
        .disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
        .registerKotlinModule()
        .registerModule(AvroJavaTimeModule())
}
