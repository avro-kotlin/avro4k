package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class FieldNamingStrategyTest : StringSpec({
    listOf(
        "___user_id__" to "___user_id__",
        "street_line" to "street_line",
        "streetLine" to "streetLine",
        "user-id" to "user-id"
    ).forEach { (input, expected) ->
        "identity: $input should produce $expected" {
            FieldNamingStrategy.Identity.format(input) shouldBe expected
        }
    }

    listOf(
        "_leading__underscore" to "leadingUnderscore",
        "a.b.c" to "aBC",
        "a1b2c3" to "a1B2C3",
        "a___b" to "aB",
        "a___b__" to "aB",
        "a_b_c" to "aBC",
        "a_bWord_ZaK" to "aBWordZaK",
        "aaa_B0c_dEee" to "aaaB0CDEee",
        "aAaaaAAaa" to "aAaaaAAaa",
        "AB_test" to "abTest",
        "ABCdd" to "abCdd",
        "ABCTest" to "abcTest",
        "already_snake_case" to "alreadySnakeCase",
        "alreadyCamelCase" to "alreadyCamelCase",
        "b0c" to "b0C",
        "HTTP2Server" to "http2Server",
        "http2server" to "http2Server",
        "JSONDataAPI" to "jsonDataApi",
        "lowerUPPER" to "lowerUpper",
        "lowerUPPERCase" to "lowerUpperCase",
        "mix_ed-words" to "mixEdWords",
        "MyXMLParser" to "myXmlParser",
        "myXMLParser" to "myXmlParser",
        "parseHTTPResponse" to "parseHttpResponse",
        "rfc3339timestamp" to "rfc3339Timestamp",
        "sha256_encoding" to "sha256Encoding",
        "sha256encoding" to "sha256Encoding",
        "snake__case" to "snakeCase",
        "street_line" to "streetLine",
        "StreetLine" to "streetLine",
        "Test123XML" to "test123Xml",
        "test123xml" to "test123Xml",
        "tesT_" to "tesT",
        "test_" to "test",
        "URL" to "url",
        "URLValue" to "urlValue",
        "user-id-number" to "userIdNumber",
        "user_id" to "userId",
        "user_v1UUID" to "userV1Uuid",
        "userIDNumber" to "userIdNumber",
        "v2_test3Case" to "v2Test3Case",
        "v2test3case" to "v2Test3Case",
        "Version2Number" to "version2Number",
        "version2number" to "version2Number",
        "version_2_number" to "version2Number",
        "XML" to "xml",
        "XMLEngine" to "xmlEngine"
    ).forEach { (input, expected) ->
        "camel case: $input should produce $expected" {
            FieldNamingStrategy.CamelCase.format(input) shouldBe expected
        }
    }

    listOf(
        "_leading__underscore" to "LeadingUnderscore",
        "a.b.c" to "ABC",
        "a1b2c3" to "A1B2C3",
        "a___b" to "AB",
        "a___b__" to "AB",
        "a_b_c" to "ABC",
        "a_bWord_ZaK" to "ABWordZaK",
        "aaa_B0c_dEee" to "AaaB0CDEee",
        "aAaaaAAaa" to "AAaaaAAaa",
        "AB_test" to "AbTest",
        "ABCdd" to "AbCdd",
        "ABCTest" to "AbcTest",
        "already_snake_case" to "AlreadySnakeCase",
        "alreadyCamelCase" to "AlreadyCamelCase",
        "b0c" to "B0C",
        "HTTP2Server" to "Http2Server",
        "http2server" to "Http2Server",
        "JSONDataAPI" to "JsonDataApi",
        "lowerUPPER" to "LowerUpper",
        "lowerUPPERCase" to "LowerUpperCase",
        "mix_ed-words" to "MixEdWords",
        "MyXMLParser" to "MyXmlParser",
        "myXMLParser" to "MyXmlParser",
        "parseHTTPResponse" to "ParseHttpResponse",
        "rfc3339timestamp" to "Rfc3339Timestamp",
        "sha256_encoding" to "Sha256Encoding",
        "sha256encoding" to "Sha256Encoding",
        "snake__case" to "SnakeCase",
        "street_line" to "StreetLine",
        "StreetLine" to "StreetLine",
        "Test123XML" to "Test123Xml",
        "test123xml" to "Test123Xml",
        "tesT_" to "TesT",
        "test_" to "Test",
        "URL" to "Url",
        "URLValue" to "UrlValue",
        "user-id-number" to "UserIdNumber",
        "user_id" to "UserId",
        "user_v1UUID" to "UserV1Uuid",
        "userIDNumber" to "UserIdNumber",
        "v2_test3Case" to "V2Test3Case",
        "v2test3case" to "V2Test3Case",
        "Version2Number" to "Version2Number",
        "version2number" to "Version2Number",
        "version_2_number" to "Version2Number",
        "XML" to "Xml",
        "XMLEngine" to "XmlEngine"
    ).forEach { (input, expected) ->
        "pascal case: $input should produce $expected" {
            input.toPascalCase() shouldBe expected
        }
    }
})