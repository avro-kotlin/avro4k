package com.github.avrokotlin.avro4k.kafka.confluent

import io.kotest.assertions.AssertionFailedError
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import io.kotest.matchers.EqualityMatcherResult
import io.kotest.matchers.Matcher
import io.kotest.matchers.MatcherResult
import io.kotest.matchers.string.shouldContain


fun <T> onThrowMatcher(test: (value: T) -> Unit) = Matcher<T> { actualValue ->
    try {
        test(actualValue)
        SuccessMatcherResult
    } catch (e: AssertionFailedError) {
        matcherErrorResult(
            actual = e.actual?.ephemeralValue,
            expected = e.expected?.ephemeralValue,
            e.message ?: "Assertion failed:\n${e.printStackTrace()}",
        )
    } catch (e: AssertionError) {
        matcherErrorResult(e.message ?: "Assertion failed:\n${e.printStackTrace()}")
    }

}

inline fun <reified T : Throwable> shouldThrowCause(block: () -> Any?) {
    val thrownCause = try {
        block()
        null
    } catch (thrown: Throwable) {
        thrown.cause
    }
    withClue("The block should throw an exception with cause of type ${T::class.qualifiedName}") {
        shouldThrow<T> { if (thrownCause != null) throw thrownCause }
    }
}

inline fun <reified T : Throwable> shouldThrowCauseWithMessageContaining(messagePart: String, block: () -> Any?) {
    val thrownCause = try {
        block()
        null
    } catch (thrown: Throwable) {
        thrown.cause
    }
    withClue("The block should throw an exception with cause of type ${T::class.qualifiedName} with message containing $messagePart") {
        shouldThrow<T> { if (thrownCause != null) throw thrownCause }
        thrownCause?.message shouldContain messagePart
    }
}

private fun matcherErrorResult(
    failureMessage: String,
): MatcherResult {
    return MatcherResult(false, { failureMessage }, { throw UnsupportedOperationException() })
}

private fun matcherErrorResult(
    actual: Any?,
    expected: Any?,
    failureMessage: String,
): MatcherResult {
    return EqualityMatcherResult(false, actual, expected, { failureMessage }, { throw UnsupportedOperationException() })
}

private object SuccessMatcherResult : MatcherResult {
    override fun failureMessage(): String = throw UnsupportedOperationException()
    override fun negatedFailureMessage(): String = throw UnsupportedOperationException()
    override fun passed(): Boolean = true
}
