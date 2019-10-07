package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.serialization.Serializable

@Serializable
sealed class Node {
   @Serializable
   data class Branch(val left: Node, val right: Node) : Node()

   @Serializable
   data class Leaf(val value: String) : Node()
}

class SealedClassSchemaTest : StringSpec({

   "!schema for sealed heirarchy" {
      val schema = Avro.default.schema(Node.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigdecimal.json"))
      schema shouldBe expected
   }
})