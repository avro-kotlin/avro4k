package com.github.avrokotlin.benchmark.simple

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.benchmark.internal.SimpleDatasClass
import kotlinx.benchmark.Benchmark
import org.apache.avro.Conversions
import org.apache.avro.data.TimeConversions
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectData
import java.io.ByteArrayInputStream
import java.io.OutputStream

internal class ApacheAvroReflectSimpleBenchmark : SerializationSimpleBenchmark() {
    lateinit var writer: DatumWriter<SimpleDatasClass>
    lateinit var encoder: Encoder
    lateinit var reader: DatumReader<SimpleDatasClass>

    lateinit var data: ByteArray
    var writeMode = false

    override fun setup() {
        ReflectData.get().addLogicalTypeConversion(Conversions.UUIDConversion())
        ReflectData.get().addLogicalTypeConversion(Conversions.DecimalConversion())
        ReflectData.get().addLogicalTypeConversion(TimeConversions.DateConversion())
        ReflectData.get().addLogicalTypeConversion(TimeConversions.TimestampMillisConversion())

        writer = ReflectData.get().createDatumWriter(schema) as DatumWriter<SimpleDatasClass>
        encoder = EncoderFactory.get().directBinaryEncoder(OutputStream.nullOutputStream(), null)

        reader = ReflectData.get().createDatumReader(schema) as DatumReader<SimpleDatasClass>
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, clients)
    }

    @Benchmark
    fun read() {
        if (writeMode) writeMode = false
        val decoder = DecoderFactory.get().directBinaryDecoder(ByteArrayInputStream(data), null)
        reader.read(null, decoder)
    }

    @Benchmark
    fun write() {
        if (!writeMode) writeMode = true
        writer.write(clients, encoder)
    }
}