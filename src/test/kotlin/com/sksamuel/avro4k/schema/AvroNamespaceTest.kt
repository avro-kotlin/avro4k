package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroNamespace
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class AvroNamespaceSchemaTest : FunSpec({

   test("support namespace annotations on records") {

      @AvroNamespace("com.yuval")
      @Serializable
      data class AnnotatedNamespace(val s: String)

      val schema = Avro.default.schema(AnnotatedNamespace.serializer())
      schema.namespace shouldBe "com.yuval"
   }

   test("support namespace annotations in nested records") {

      @AvroNamespace("com.yuval.internal")
      @Serializable
      data class InternalAnnotated(val i: Int)

      @AvroNamespace("com.yuval")
      @Serializable
      data class AnnotatedNamespace(val s: String, val internal: InternalAnnotated)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/namespace.json"))
      val schema = Avro.default.schema(AnnotatedNamespace.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("support namespace annotations on field") {

      @Serializable
      data class InternalAnnotated(val i: Int)

      @Serializable
      @AvroNamespace("com.yuval")
      data class AnnotatedNamespace(val s: String,
                                    @AvroNamespace("com.yuval.internal") val internal: InternalAnnotated)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/namespace.json"))
      val schema = Avro.default.schema(AnnotatedNamespace.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("favour namespace annotations on field over record") {

      @Serializable
      @AvroNamespace("ignore")
      data class InternalAnnotated(val i: Int)

      @Serializable
      @AvroNamespace("com.yuval")
      data class AnnotatedNamespace(val s: String,
                                    @AvroNamespace("com.yuval.internal") val internal: InternalAnnotated)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/namespace.json"))
      val schema = Avro.default.schema(AnnotatedNamespace.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("empty namespace") {

      @AvroNamespace("")
      @Serializable
      data class Foo(val s: String)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/namespace_empty.json"))
      val schema = Avro.default.schema(Foo.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

})