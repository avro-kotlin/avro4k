package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class NameStrategyTest : StringSpec({
    "identity keeps input untouched and identifier stable" {
        listOf(
            "street_line" to "street_line",
            "streetLine" to "streetLine",
            "user-id" to "user-id",
            "___user_id__" to "___user_id__"
        ).forEach { (input, expected) ->
            NameStrategy.IDENTITY.format(input) shouldBe expected
        }
        NameStrategy.IDENTITY.identifier shouldBe "identity"
    }

    "camel case produces lower camel case" {
        listOf(
            "street_line" to "streetLine",
            "StreetLine" to "streetLine",
            "user-id" to "userId",
            "URLValue" to "uRLValue"
        ).forEach { (input, expected) ->
            NameStrategy.CAMEL_CASE.format(input) shouldBe expected
        }
        NameStrategy.CAMEL_CASE.identifier shouldBe "camelCase"
    }

    "snake case keeps underscores and splits camel case boundaries" {
        listOf(
            "streetLine" to "street_line",
            "street_line" to "street_line",
            "UserId" to "user_id",
            "URLValue" to "urlvalue",
            "  leading__Spaces  " to "leading_spaces",
            "name-with-dash" to "name_with_dash"
        ).forEach { (input, expected) ->
            NameStrategy.SNAKE_CASE.format(input) shouldBe expected
        }
        NameStrategy.SNAKE_CASE.identifier shouldBe "snakeCase"
    }

    "pascal case produces upper camel case" {
        listOf(
            "street_line" to "StreetLine",
            "streetLine" to "StreetLine",
            "user-id" to "UserId",
            "URLValue" to "URLValue"
        ).forEach { (input, expected) ->
            NameStrategy.PASCAL_CASE.format(input) shouldBe expected
        }
        NameStrategy.PASCAL_CASE.identifier shouldBe "pascalCase"
    }

    "custom strategy uses provided identifier and formatter" {
        val custom =
            NameStrategy.custom("strip-x-camel") { original ->
                NameStrategy.CAMEL_CASE.format(original.removePrefix("x_"))
            }

        custom.identifier shouldBe "strip-x-camel"
        custom.format("x_street_line") shouldBe "streetLine"
        custom.format("street_line") shouldBe "streetLine"
    }
})