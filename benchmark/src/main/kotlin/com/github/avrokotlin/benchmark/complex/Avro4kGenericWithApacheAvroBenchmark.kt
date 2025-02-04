package com.github.avrokotlin.benchmark.complex

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromGenericData
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.benchmark.internal.Clients
import kotlinx.benchmark.Benchmark
import kotlinx.serialization.ExperimentalSerializationApi
import org.apache.avro.Conversions
import org.apache.avro.generic.GenericData
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import java.io.OutputStream

internal class Avro4kGenericWithApacheAvroBenchmark : SerializationBenchmark() {
    lateinit var writer: DatumWriter<Any?>
    lateinit var encoder: Encoder
    lateinit var reader: DatumReader<Any?>

    lateinit var data: ByteArray

    override fun setup() {
        GenericData.get().addLogicalTypeConversion(Conversions.DecimalConversion())
//        GenericData.get().addLogicalTypeConversion(TimeConversions.DateConversion())
//        GenericData.get().addLogicalTypeConversion(TimeConversions.TimestampMillisConversion())

        writer = GenericData.get().createDatumWriter(schema) as DatumWriter<Any?>
        encoder = EncoderFactory.get().directBinaryEncoder(OutputStream.nullOutputStream(), null)

        reader = GenericData.get().createDatumReader(schema) as DatumReader<Any?>
    }

    override fun prepareBinaryData() {
        data = Avro.encodeToByteArray(schema, clients)
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Benchmark
    fun read() {
        val decoder = DecoderFactory.get().binaryDecoder(data, null)
        val genericData = reader.read(null, decoder)
        Avro.decodeFromGenericData<Clients>(schema, genericData)
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Benchmark
    fun write() {
        val genericData = Avro.encodeToGenericData(schema, clients)
        writer.write(genericData, encoder)
    }
}