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
        val cases =
            listOf(
                "aaa_B0c_dEee" to "aaaB0cDEee",
                "aAaaaAAaa" to "aAaaaAAaa",
                "ABCdd" to "abCdd",
                "test_" to "test",
                "tesT_" to "tesT",
                "XML" to "xml",
                "Test123XML" to "test123Xml",
                "AB_test" to "abTest",
                "a___b" to "aB",
                "a___b__" to "aB",
                "ABCTest" to "abcTest",
                "JSONDataAPI" to "jsonDataApi",
                "lowerUPPER" to "lowerUpper",
                "lowerUPPERCase" to "lowerUpperCase",
                "myXMLParser" to "myXmlParser",
                "MyXMLParser" to "myXmlParser",
                "parseHTTPResponse" to "parseHttpResponse",
                "URL" to "url",
                "URLValue" to "urlValue",
                "userIDNumber" to "userIdNumber",
                "XMLEngine" to "xmlEngine",
                "_leading__underscore" to "leadingUnderscore",
                "a_b_c" to "aBC",
                "a_bWord_ZaK" to "aBWordZaK",
                "a.b.c" to "aBC",
                "alreadyCamelCase" to "alreadyCamelCase",
                "already_snake_case" to "alreadySnakeCase",
                "mix_ed-words" to "mixEdWords",
                "snake__case" to "snakeCase",
                "street_line" to "streetLine",
                "StreetLine" to "streetLine",
                "user-id-number" to "userIdNumber",
                "user_id" to "userId",
                "HTTP2Server" to "http2Server",
                "v2_test3Case" to "v2Test3Case",
                "Version2Number" to "version2Number"
            )

        cases.forEach { (input, expected) ->
            FieldNamingStrategy.CamelCase.format(input) shouldBe expected
        }
    }
})