package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.getAvroName
import com.github.avrokotlin.avro4k.schema.DefaultNamingStrategy
import com.github.avrokotlin.avro4k.schema.getTypeNamed
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.nullable
import org.apache.avro.generic.GenericRecordBuilder


@OptIn(ExperimentalSerializationApi::class)
class UnionEncoderTest : WordSpec({

    "UnionEncoder" should {
        "encode polymorphic types child1"  {
            val schema = Avro.default.schema(Parent.serializer().nullable)

            val record = Avro.default.encodeToGenericData<Parent>(schema, Child1(17, true))
            record shouldBe GenericRecordBuilder(schema.getTypeNamed(Child1.serializer().descriptor.getAvroName(DefaultNamingStrategy)))
                    .set("i", 17)
                    .set("toto", true)
                    .build()
        }
        "encode polymorphic types child2"  {
            val schema = Avro.default.schema(Parent.serializer().nullable)

            println(schema)

            val record = Avro.default.encodeToGenericData<Parent>(schema, Child2("hello", 56.12, null))
            record shouldBe GenericRecordBuilder(schema.getTypeNamed(Child2.serializer().descriptor.getAvroName(DefaultNamingStrategy)))
                    .set("s", "hello")
                    .set("z", 56.12)
                    .set("toto", null)
                    .build()
        }
        "encode polymorphic types null"  {
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
