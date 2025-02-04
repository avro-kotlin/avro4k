package com.github.avrokotlin.benchmark.lists

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.benchmark.internal.ListWrapperDatasClass
import kotlinx.benchmark.Benchmark
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectData
import java.io.OutputStream

internal class ApacheAvroReflectListsBenchmark : SerializationListsBenchmark() {
    lateinit var writer: DatumWriter<ListWrapperDatasClass>
    lateinit var encoder: Encoder
    lateinit var reader: DatumReader<ListWrapperDatasClass>

    lateinit var data: ByteArray

    override fun setup() {
        writer = ReflectData.get().createDatumWriter(schema) as DatumWriter<ListWrapperDatasClass>
        encoder = EncoderFactory.get().directBinaryEncoder(OutputStream.nullOutputStream(), null)

        reader = ReflectData.get().createDatumReader(schema) as DatumReader<ListWrapperDatasClass>
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, lists)
    }

    @Benchmark
    fun read() {
        val decoder = DecoderFactory.get().binaryDecoder(data, null)
        reader.read(null, decoder)
    }

    @Benchmark
    fun write() {
        writer.write(lists, encoder)
    }
}