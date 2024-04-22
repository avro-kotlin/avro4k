package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable

@Serializable
private data class County(val name: String, val towns: List<Town>, val ceremonial: Boolean, val lat: Double, val long: Double)

@Serializable
private data class Town(val name: String, val population: Int)

@Serializable
private data class Birthplace(val person: String, val town: Town)

@Serializable
private data class PersonV2(
    val name: String,
    val hasChickenPoxVaccine: Boolean,
    @AvroDefault("null")
    val hasCovidVaccine: Boolean? = null,
)

class NestedClassEncodingTest : StringSpec({
    "decode nested class" {
        AvroAssertions.assertThat(Birthplace(person = "Sammy Sam", town = Town(name = "Hardwick", population = 123)))
            .isEncodedAs(
                record(
                    "Sammy Sam",
                    record("Hardwick", 123)
                )
            )
    }

    "decode nested list of classes" {
        AvroAssertions.assertThat(
            County(
                name = "Bucks",
                towns = listOf(Town(name = "Hardwick", population = 123), Town(name = "Weedon", population = 225)),
                ceremonial = true,
                lat = 12.34,
                long = 0.123
            )
        ).isEncodedAs(
            record(
                "Bucks",
                listOf(
                    record("Hardwick", 123),
                    record("Weedon", 225)
                ),
                true,
                12.34,
                0.123
            )
        )
    }

    "decode nested class from previous schema (new schema is back compat)" {
        AvroAssertions.assertThat(
            PersonV2(name = "Ryan", hasChickenPoxVaccine = true, hasCovidVaccine = null)
        ).isEncodedAs(
            record("Ryan", true, null)
        )
    }
})