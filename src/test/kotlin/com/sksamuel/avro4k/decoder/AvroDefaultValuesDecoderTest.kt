package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroDefault
import com.sksamuel.avro4k.AvroName
import com.sksamuel.avro4k.io.AvroFormat
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToByteArray
import org.apache.avro.generic.GenericData

@Serializable
data class FooElement(
   val content : String
)

@Serializable
@AvroName("container")
data class ContainerWithoutDefaultFields(
   val name : String
)
@Serializable
@AvroName("container")
data class ContainerWithDefaultFields(
   val name : String,
   @AvroDefault("hello")
   val strDefault : String,
   @AvroDefault("1")
   val intDefault : Int,
   @AvroDefault("true")
   val booleanDefault : Boolean,
   @AvroDefault("1.23")
   val doubleDefault : Double,
   @AvroDefault(Avro.NULL)
   val nullableStr : String?,
   @AvroDefault("""{"content":"foo"}""")
   val foo : FooElement,
   @AvroDefault("[]")
   val emptyFooList : List<FooElement>,
   @AvroDefault("""[{"content":"bar"}]""")
   val filledFooList : List<FooElement>
)

class AvroDefaultValuesDecoderTest : FunSpec({
   test("test default values correctly decoded") {
      val name = "abc"
      val schema = Avro.default.schema(ContainerWithoutDefaultFields.serializer())
      val record = GenericData.Record(schema)
      record.put("name", name)

      val byteArray = Avro.default.encodeToByteArray(ContainerWithoutDefaultFields("abc"))
      val deserialized = Avro.default.openInputStream(ContainerWithDefaultFields.serializer()){
         format = AvroFormat.DataFormat
         readerSchema = Avro.default.schema(ContainerWithDefaultFields.serializer())
      }.from(byteArray).nextOrThrow()
      deserialized.name.shouldBe("abc")
      deserialized.strDefault.shouldBe("hello")
      deserialized.intDefault.shouldBe(1)
      deserialized.booleanDefault.shouldBe(true)
      deserialized.doubleDefault.shouldBe(1.23)
      deserialized.nullableStr.shouldBeNull()
      deserialized.foo.content.shouldBe("foo")
      deserialized.emptyFooList.shouldBeEmpty()
      deserialized.filledFooList.shouldContainExactly(FooElement("bar"))
   }
})