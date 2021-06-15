package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

class FixedDecoderTest : FunSpec({

  test("decode bytes to String") {
    val schema = SchemaBuilder.record("FixedString").fields()
        .name("z").type(Schema.createFixed("z", null, "ns", 10)).noDefault()
        .endRecord()
    val record = GenericData.Record(schema)
    record.put("z", byteArrayOf(115, 97, 109))
    Avro.default.fromRecord(FixedString.serializer(), record) shouldBe FixedString("sam")
  }

//  test("support nullables of fixed") {
//    val schema = AvroSchema[NullableFixedType]
//    val record = new GenericData . Record (schema)
//    record.put("z", new GenericData . Fixed (AvroSchema[FixedValueType], Array[Byte](115, 97, 109)))
//    Decoder[NullableFixedType].decode(record, schema, DefaultFieldMapper) shouldBe NullableFixedType(Some(
//        FixedValueType("sam")))
//  }

}) {

  @Serializable
  data class FixedString(val z: String)

  @Serializable
  data class NullableFixedType(val z: FixedString?)
}
