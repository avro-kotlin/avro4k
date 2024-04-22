package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroLogicalType
import com.github.avrokotlin.avro4k.AvroLogicalTypeSupplier
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.LogicalType
import org.apache.avro.Schema

class AvroLogicalTypeTest : StringSpec({
    "@AvroLogicalType annotation should be supported" {
        AvroAssertions.assertThat<Something>()
            .generatesSchema(CustomLogicalType.addToSchema(Schema.create(Schema.Type.STRING)))
    }
}) {
    @JvmInline
    @Serializable
    private value class Something(
        @AvroLogicalType(CustomLogicalTypeSupplier::class) val value: String,
    )

    private object CustomLogicalType : LogicalType("custom")

    object CustomLogicalTypeSupplier : AvroLogicalTypeSupplier {
        override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
            return CustomLogicalType
        }
    }
}