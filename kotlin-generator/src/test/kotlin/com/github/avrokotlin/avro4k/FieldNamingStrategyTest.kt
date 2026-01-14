package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class FieldNamingStrategyTest : StringSpec({
    "identity keeps input untouched" {
        listOf(
            "street_line" to "street_line",
            "streetLine" to "streetLine",
            "user-id" to "user-id",
            "___user_id__" to "___user_id__"
        ).forEach { (input, expected) ->
            FieldNamingStrategy.Identity.format(input) shouldBe expected
        }
    }

    "camel case produces lower camel case" {
        listOf(
            "a_b_c" to "aBC",
            "a_bWord_ZaK" to "aBWordZaK",
            "URLValue" to "urlValue",
            "userIDNumber" to "userIdNumber",
            "MyXMLParser" to "myXmlParser",
            "parseHTTPResponse" to "parseHttpResponse",
            "_leading__underscore" to "leadingUnderscore",
            "already_snake_case" to "alreadySnakeCase",
            "street_line" to "streetLine",
            "StreetLine" to "streetLine",
            "user_id" to "userId"
        ).forEach { (input, expected) ->
            FieldNamingStrategy.CamelCase.format(input) shouldBe expected
        }
    }
})