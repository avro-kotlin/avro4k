package avro4k.examples

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.schema
import example.avro4k.namings.BillingAddress
import example.avro4k.namings.NamingStrategyRecord

fun main() {
    // NameStrategy.SNAKE_CASE applied by the plugin: generated properties are snake_cased.
    val record =
        NamingStrategyRecord(
            user_id = "user-123",
            billing_address = BillingAddress(street_line = "123 Snake Rd")
        )

    val bytes = Avro.encodeToByteArray(Avro.schema<NamingStrategyRecord>(), record)
    println("Encoded ${bytes.size} bytes for NamingStrategyRecord with snake_cased properties.")
}
