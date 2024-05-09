package com.github.avrokotlin.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectReader
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.dataformat.avro.AvroFactory
import com.fasterxml.jackson.dataformat.avro.AvroMapper
import com.fasterxml.jackson.dataformat.avro.jsr310.AvroJavaTimeModule
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Setup

internal class JacksonAvroClientsBenchmark : SerializationBenchmark() {
    lateinit var writer: ObjectWriter
    lateinit var reader: ObjectReader

    lateinit var data: ByteArray
    var writeMode = false

    @Setup
    fun setup() {
        val schemaMapper = ObjectMapper(AvroFactory())
            .registerKotlinModule()
            .registerModule(AvroJavaTimeModule())
        writer = Clients::class.java.createWriter(schemaMapper)
        reader = Clients::class.java.createReader(schemaMapper)
    }

    override fun prepareBinaryData() {
        data = writer.writeValueAsBytes(clients)
    }

    @Benchmark
    fun read() {
        if (writeMode) writeMode = false
        reader.readValue<Clients>(data)
    }

    @Benchmark
    fun write() {
        if (!writeMode) writeMode = true
        writer.writeValueAsBytes(clients)
    }
}

private fun <T> Class<T>.createWriter(schemaMapper: ObjectMapper): ObjectWriter {
    val gen = AvroSchemaGenerator()
    schemaMapper.acceptJsonFormatVisitor(this, gen)

    val mapper = AvroMapper().registerModule(kotlinModule()).registerModule(AvroJavaTimeModule())
    return mapper.writer(gen.generatedSchema)
}

private fun <T> Class<T>.createReader(schemaMapper: ObjectMapper): ObjectReader {
    val gen = AvroSchemaGenerator()
    schemaMapper.acceptJsonFormatVisitor(this, gen)

    val mapper = AvroMapper().registerModule(kotlinModule()).registerModule(AvroJavaTimeModule())
    return mapper.reader(gen.generatedSchema).forType(this)
}
