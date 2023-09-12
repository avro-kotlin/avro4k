@file:UseSerializers(BigIntegerSerializer::class)

package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.serializer.BigIntegerSerializer
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.util.Utf8
import java.math.BigInteger

class BigIntegerEncoderTest : FunSpec({

   test("use string for bigint") {

      val schema = Avro.default.schema(BigIntegerTest.serializer())

      val test = BigIntegerTest(BigInteger("123123123123213213213123214325365477686789676234"))

       Avro.default.encodeToGenericData(test, schema) shouldBeContentOf ListRecord(schema, Utf8("123123123123213213213123214325365477686789676234"))
   }

   test("encode nullable big ints") {

      val schema = Avro.default.schema(NullableBigIntegerTest.serializer())
       Avro.default.encodeToGenericData(
           NullableBigIntegerTest(BigInteger("12312312312321312365477686789676234")),
           schema
       ) shouldBeContentOf
         ListRecord(schema, Utf8("12312312312321312365477686789676234"))
       Avro.default.encodeToGenericData(NullableBigIntegerTest(null), schema) shouldBeContentOf ListRecord(schema, null)

   }
}) {
   @Serializable
   data class BigIntegerTest(val b: BigInteger)


   @Serializable
   data class NullableBigIntegerTest(val b: BigInteger?)
}
