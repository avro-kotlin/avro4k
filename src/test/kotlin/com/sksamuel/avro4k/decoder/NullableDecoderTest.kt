package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData

@Serializable
data class NullableBoolean(val b: Boolean?)

@Serializable
data class NullableString(val s: String?)

@Serializable
data class NullableNestedRecord(val nl: List<NullableString>?)

class NullableDecoderTest : WordSpec({

  "Decoder" should {
    "support nullable strings"  {
      val schema = Avro.default.schema(NullableString.serializer())

      val record1 = GenericData.Record(schema)
      record1.put("s", "hello")
      Avro.default.fromRecord(NullableString.serializer(), record1) shouldBe NullableString("hello")

      val record2 = GenericData.Record(schema)
      record2.put("s", null)
      Avro.default.fromRecord(NullableString.serializer(), record2) shouldBe NullableString(null)
    }
//    "support decoding required fields as Option" in {
//      val requiredStringSchema = AvroSchema[RequiredString]
//
//      val requiredStringRecord = new GenericData . Record (requiredStringSchema)
//      requiredStringRecord.put("s", "hello")
//      Decoder[OptionString].decode(requiredStringRecord,
//          requiredStringSchema,
//          DefaultFieldMapper) shouldBe OptionString(Some("hello"))
//    }
    "support nullable booleans"  {
      val schema = Avro.default.schema(NullableBoolean.serializer())

      val record1 = GenericData.Record(schema)
      record1.put("b", true)
      Avro.default.fromRecord(NullableBoolean.serializer(), record1) shouldBe NullableBoolean(true)

      val record2 = GenericData.Record(schema)
      record2.put("b", null)
      Avro.default.fromRecord(NullableBoolean.serializer(), record2) shouldBe NullableBoolean(null)
    }
//    "if a field is missing, use default value" in {
//      val schema = AvroSchema[OptionStringDefault]
//
//      val record1 = new GenericData . Record (AvroSchema[SchemaWithoutExpectedField])
//
//      Decoder[OptionStringDefault].decode(record1,
//          schema,
//          DefaultFieldMapper) shouldBe OptionStringDefault(Some("cupcat"))
//    }
//    "if an enum field is missing, use default value" in {
//      val schema = AvroSchema[OptionEnumDefault]
//
//      val record1 = new GenericData . Record (AvroSchema[SchemaWithoutExpectedField])
//      Decoder[OptionEnumDefault].decode(record1, schema, DefaultFieldMapper) shouldBe OptionEnumDefault(Some(
//          CuppersOptionEnum))
//    }
   "support nullable List of nested records" {
      val nullableStringSchema = Avro.default.schema(NullableString.serializer())
      val nullableListSchema = Avro.default.schema(NullableNestedRecord.serializer())

      val string = GenericData.Record(nullableStringSchema)
      string.put("s", "hello")
      val record1 = GenericData.Record(nullableListSchema)
      record1.put("nl", listOf(string))

      Avro.default.fromRecord(NullableNestedRecord.serializer(), record1) shouldBe NullableNestedRecord(listOf(NullableString("hello")))

      val record2 = GenericData.Record(nullableListSchema)
      record2.put("nl", null)
      Avro.default.fromRecord(NullableNestedRecord.serializer(), record2) shouldBe NullableNestedRecord(null)
   }
  }

})