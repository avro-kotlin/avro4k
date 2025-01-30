package com.github.avrokotlin.benchmark.lists

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
import com.github.avrokotlin.benchmark.internal.ListWrapperDatasClass
import kotlinx.benchmark.Benchmark
import java.io.OutputStream


internal class JacksonAvroListsBenchmark : SerializationListsBenchmark() {
    lateinit var writer: ObjectWriter
    lateinit var reader: ObjectReader

    lateinit var data: ByteArray
    var writeMode = false

    override fun setup() {
        writer = ListWrapperDatasClass::class.java.createWriter()
        reader = ListWrapperDatasClass::class.java.createReader()
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, lists)
    }

    @Benchmark
    fun read() {
        if (writeMode) writeMode = false
        reader.readValue<ListWrapperDatasClass>(data)
    }

    @Benchmark
    fun write() {
        if (!writeMode) writeMode = true
        writer.writeValue(OutputStream.nullOutputStream(), lists)
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
