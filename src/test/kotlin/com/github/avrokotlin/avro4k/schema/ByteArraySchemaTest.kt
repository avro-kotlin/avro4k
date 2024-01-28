package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroName
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class ByteArraySchemaTest : FunSpec({

    test("encode byte arrays as BYTES type") {

        val schema = Avro.default.schema(ByteArrayTest.serializer())
        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/byte_array.json"))
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("encode lists as BYTES type") {

        val schema = Avro.default.schema(ByteListTest.serializer())
        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/byte_array.json"))
        schema.toString(true) shouldBe expected.toString(true)
    }

//  test("support top level byte arrays") {
//    @Serializable
//    val schema = Avro.default.schema(Array[Byte.serializer())]
//    val expected = new org . apache . avro . Schema . Parser ().parse(getClass.getResourceAsStream("/top_level_byte_array.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }

//  test("encode ByteBuffer as BYTES type") {
//    @Serializable
//    data class Test(val z: ByteBuffer)
//
//    val schema = Avro.default.schema(Test.serializer())
//    val expected = new org . apache . avro . Schema . Parser ().parse(getClass.getResourceAsStream("/bytebuffer.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }

//  test("support top level ByteBuffers") {
//    val schema = Avro.default.schema(ByteBuffer.serializer())
//    val expected = new org . apache . avro . Schema . Parser ().parse(getClass.getResourceAsStream("/top_level_bytebuffer.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }
}) {
    @Serializable
    data class ByteArrayTest(val z: ByteArray)

    @Serializable
    @AvroName("ByteArrayTest")
    data class ByteListTest(val z: List<Byte>)
}