package avro4k.examples

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.schema
import example.avro4k.namings.BillingAddress
import example.avro4k.namings.NamingStrategyRecord

fun main() {
    // NameStrategy.CAMEL_CASE applied by the plugin: generated properties are camelCased.
    val record =
        NamingStrategyRecord(
            userId = "user-123",
            billingAddress = BillingAddress(streetLine = "123 Snake Rd")
        )

    val bytes = Avro.encodeToByteArray(Avro.schema<NamingStrategyRecord>(), record)
    println("Encoded ${bytes.size} bytes for NamingStrategyRecord with camelCased properties.")
}
