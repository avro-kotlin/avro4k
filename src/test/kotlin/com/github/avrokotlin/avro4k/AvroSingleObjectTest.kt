package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.apache.avro.SchemaNormalization
import java.time.Instant

class AvroSingleObjectTest : StringSpec({
    val orderEvent =
        OrderEvent(
            OrderId("123"),
            Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS),
            42.0
        )
    val schema = Avro.schema(OrderEvent.serializer())
    val schemas = mapOf(SchemaNormalization.parsingFingerprint64(schema) to schema)
    val avroSingleObject = AvroSingleObject(schemas::get)

    "support writing avro single object" {
        // write with avro4k
        val bytes = avroSingleObject.encodeToByteArray(orderEvent)

        // check
        bytes[0] shouldBe 0xC3.toByte()
        bytes[1] shouldBe 1
        bytes.sliceArray(2..9) shouldBe SchemaNormalization.parsingFingerprint("CRC-64-AVRO", schema)
        bytes.sliceArray(10 until bytes.size) shouldBe Avro.encodeToByteArray(orderEvent)
    }

    "support reading avro single object" {
        // write with apache avro lib
        val bytes =
            byteArrayOf(0xC3.toByte(), 1) +
                SchemaNormalization.parsingFingerprint("CRC-64-AVRO", schema) +
                Avro.encodeToByteArray(orderEvent)

        val decoded = avroSingleObject.decodeFromByteArray<OrderEvent>(bytes)

        decoded shouldBe orderEvent
    }
}) {
    @Serializable
    private data class OrderEvent(
        val orderId: OrderId,
        @Contextual val date: Instant,
        val amount: Double,
    )

    @Serializable
    @JvmInline
    private value class OrderId(val value: String)
}