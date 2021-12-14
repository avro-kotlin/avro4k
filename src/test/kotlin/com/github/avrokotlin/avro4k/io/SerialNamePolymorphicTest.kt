package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic


class SerialNamePolymorphicTest : StringSpec ({
  "roundtrip without explicit registration" {
      writeRead(FooType1, Foo.serializer())
  }
    "roundtrip with explicit registration" {
        val avro = Avro(SerializersModule{
            polymorphic(Foo::class) {
                subclass(FooType1::class, FooType1.serializer())
                subclass(FooType2::class, FooType2.serializer())
                subclass(FooWithValue::class, FooWithValue.serializer())
            }
        })
        writeRead(FooType1, Foo.serializer(), avro)
    }
    "referencing foo" {
        writeRead(ReferencingFoo(FooType1), ReferencingFoo.serializer())
    }
}) {

}
@Serializable
sealed class Foo(val type: String)

@Serializable @SerialName("type1")
object FooType1 : Foo("type1")

@Serializable @SerialName("type2")
object FooType2 : Foo("type2")

@Serializable @SerialName("withValue")
data class FooWithValue(val value: Int) : Foo("withValue")

@Serializable
data class ReferencingFoo(val foo : Foo)