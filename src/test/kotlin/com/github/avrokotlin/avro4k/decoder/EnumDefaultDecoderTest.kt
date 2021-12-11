package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroEnumDefault
import kotlinx.serialization.Serializable
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe


class EnumDefaultDecoderTest : FunSpec({
   test("Decoding enum with an unknown or future value uses default value") {
      val encoded = Avro.default.encodeToByteArray(FutureWrap.serializer(), FutureWrap(FutureEnumWithDefault.C))
      val decoded = Avro.default.decodeFromByteArray(Wrap.serializer(), encoded)

      decoded shouldBe Wrap(EnumWithDefault.UNKNOWN)
   }
}) {
   @Serializable
   @AvroEnumDefault("UNKNOWN")
   enum class EnumWithDefault {
      UNKNOWN, A, B
   }

   @Serializable
   @AvroEnumDefault("UNKNOWN")
   enum class FutureEnumWithDefault {
      UNKNOWN, A, B, C
   }

   @Serializable
   data class Wrap(val value: EnumWithDefault)

   @Serializable
   data class FutureWrap(val value: FutureEnumWithDefault)
}
