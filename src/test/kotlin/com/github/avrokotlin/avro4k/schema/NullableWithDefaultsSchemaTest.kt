package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroConfiguration
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class NullableWithDefaultsSchemaTest : FunSpec({
   test("generate null as Union[T, Null] with default null") {
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/nullables-with-defaults.json"))
      val schema = Avro(AvroConfiguration(implicitNulls = true)).schema(Test.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
}) {
   @Serializable
   data class Test(val nullableString: String?, val nullableBoolean: Boolean?)
}
