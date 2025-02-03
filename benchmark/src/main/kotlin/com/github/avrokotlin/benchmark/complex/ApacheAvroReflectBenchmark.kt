package com.github.avrokotlin.benchmark.complex

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.benchmark.internal.Clients
import kotlinx.benchmark.Benchmark
import org.apache.avro.Conversions
import org.apache.avro.data.TimeConversions
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectData
import java.io.OutputStream

internal class ApacheAvroReflectBenchmark : SerializationBenchmark() {
    lateinit var writer: DatumWriter<Clients>
    lateinit var encoder: Encoder
    lateinit var reader: DatumReader<Clients>

    lateinit var data: ByteArray

    override fun setup() {
        ReflectData.get().addLogicalTypeConversion(Conversions.UUIDConversion())
        ReflectData.get().addLogicalTypeConversion(Conversions.DecimalConversion())
        ReflectData.get().addLogicalTypeConversion(TimeConversions.DateConversion())
        ReflectData.get().addLogicalTypeConversion(TimeConversions.TimestampMillisConversion())

        writer = ReflectData.get().createDatumWriter(schema) as DatumWriter<Clients>
        encoder = EncoderFactory.get().directBinaryEncoder(OutputStream.nullOutputStream(), null)

        reader = ReflectData.get().createDatumReader(schema) as DatumReader<Clients>
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, clients)
    }

    @Benchmark
    fun read() {
        val decoder = DecoderFactory.get().binaryDecoder(data, null)
        reader.read(null, decoder)
    }

    @Benchmark
    fun write() {
        writer.write(clients, encoder)
    }
}