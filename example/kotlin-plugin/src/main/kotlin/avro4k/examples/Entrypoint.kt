package avro4k.examples

import MyKeySchema
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.schema
import example.avro4k.MyValueSchema
import shared.SharedRecord
import java.util.UUID

fun main() {
    val keyBytes = Avro.encodeToByteArray(Avro.schema<MyKeySchema>(), MyKeySchema(UUID.randomUUID()))
    val valueBytes = Avro.encodeToByteArray(Avro.schema<MyValueSchema>(), MyValueSchema(name = "example", order = 42, reference = SharedRecord("data")))
}