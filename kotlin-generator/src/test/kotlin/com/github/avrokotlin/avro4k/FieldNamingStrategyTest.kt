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

    "snake case produces lower snake case" {
        listOf(
            "a_b_c" to "a_b_c",
            "a_bWord_ZaK" to "a_b_word_za_k",
            "URLValue" to "url_value",
            "userIDNumber" to "user_id_number",
            "MyXMLParser" to "my_xml_parser",
            "parseHTTPResponse" to "parse_http_response",
            "_leading__underscore" to "leading_underscore",
            "already_snake_case" to "already_snake_case",
            "streetLine" to "street_line",
            "UserId" to "user_id"
        ).forEach { (input, expected) ->
            FieldNamingStrategy.SnakeCase.format(input) shouldBe expected
        }
    }

    "pascal case produces upper camel case" {
        listOf(
            "a_b_c" to "ABC",
            "a_bWord_ZaK" to "ABWordZaK",
            "URLValue" to "UrlValue",
            "userIDNumber" to "UserIdNumber",
            "MyXMLParser" to "MyXmlParser",
            "parseHTTPResponse" to "ParseHttpResponse",
            "_leading__underscore" to "LeadingUnderscore",
            "already_snake_case" to "AlreadySnakeCase",
            "street_line" to "StreetLine",
            "streetLine" to "StreetLine",
            "user_id" to "UserId"
        ).forEach { (input, expected) ->
            FieldNamingStrategy.PascalCase.format(input) shouldBe expected
        }
    }
})