package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.MutableListRecord
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.schema.getTypeNamed
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.nullable


@OptIn(ExperimentalSerializationApi::class)
class UnionEncoderTest : WordSpec({

    "UnionEncoder" should {
        "encode polymorphic types child1" {
            val schema = Avro.default.schema(Parent.serializer().nullable)

            val record = Avro.default.encodeToGenericData<Parent>(schema, Child1(17, true))
            record shouldBeContentOf MutableListRecord(
                schema.getTypeNamed(
                    Avro.default.nameResolver.resolveTypeName(
                        Child1.serializer().descriptor
                    )
                )!!
            ) {
                this["i"] = 17
                this["toto"] = true
            }
        }
        "encode polymorphic types child2" {
            val schema = Avro.default.schema(Parent.serializer().nullable)

            val record = Avro.default.encodeToGenericData<Parent>(schema, Child2("hello", 56.12, null))
            record shouldBeContentOf MutableListRecord(
                schema.getTypeNamed(
                    Avro.default.nameResolver.resolveTypeName(
                        Child2.serializer().descriptor
                    )
                )!!
            ) {
                this["s"] = "hello"
                this["z"] = 56.12
                this["toto"] = null
            }
        }
        "encode polymorphic types null" {
            val schema = Avro.default.schema(Parent.serializer().nullable)

            val record = Avro.default.encodeToGenericData<Parent?>(schema, null)
            record shouldBe null
        }
    }

}) {
    @Serializable
    sealed interface Parent {
        val toto: Boolean?
    }

    @Serializable
    data class Child1(val i: Int, override val toto: Boolean?) : Parent

    @Serializable
    data class Child2(val s: String, val z: Double, override val toto: Boolean?) : Parent
}
