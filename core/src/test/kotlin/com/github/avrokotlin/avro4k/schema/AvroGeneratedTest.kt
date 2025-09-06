package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.AvroGenerated
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.schema
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.Schema

class AvroGeneratedTest : StringSpec() {
    init {
        "a class with an unrelated schema in @AvroGenerated should return that schema even if invalid" {
            Avro.schema<TestValueWithUnrelatedSchema>() shouldBe Schema.Parser().parse(SCHEMA)
            Avro.schema<TestValueWithUnrelatedSchema?>() shouldBe Schema.Parser().parse(SCHEMA).nullable
        }
    }
}

private const val SCHEMA = """
{
  "type": "record",
  "name": "TestRecord",
  "namespace": "com.github.avrokotlin.avro4k.schema",
  "fields": [
    {
      "name": "value",
      "type": "string"
    }
  ]
}
"""

@JvmInline
@Serializable
@AvroGenerated(SCHEMA)
private value class TestValueWithUnrelatedSchema(val value: String)