package com.sksamuel.avro4k.streams

sealed class AvroFormat {
   object BinaryFormat : AvroFormat()
   object JsonFormat : AvroFormat()
   object DataFormat : AvroFormat()
}