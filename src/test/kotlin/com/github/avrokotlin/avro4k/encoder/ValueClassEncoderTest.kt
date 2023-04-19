package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.schema.*
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.spec.style.stringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import org.apache.avro.util.Utf8
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1

class ValueClassEncoderTest : StringSpec({
    include(valueClassEncodeTest(StringWrapper("blub")))
    include(valueClassEncodeTest(IntWrapper(1)))
    include(valueClassEncodeTest(LongWrapper(1L)))
    include(valueClassEncodeTest(ByteWrapper(1)))
    include(valueClassEncodeTest(ShortWrapper(1)))
    include(valueClassEncodeTest(BooleanWrapper(true)))
    include(valueClassEncodeTest(FloatWrapper(1f)))
    include(valueClassEncodeTest(DoubleWrapper(1.02)))
    "encode referenced value class" {

        val id = ValueClassSchemaTest.StringWrapper("100500")
        val uuid = UUID.randomUUID()
        val uuidStr = uuid.toString()
        val uuidW = ValueClassSchemaTest.UuidWrapper(uuid)
        val schema = Avro.default.schema(ValueClassSchemaTest.ContainsInlineTest.serializer())
        Avro.default.toAvroCompatibleType(
            ValueClassSchemaTest.ContainsInlineTest.serializer(),
            ValueClassSchemaTest.ContainsInlineTest(id, uuidW)
        ) shouldBe ListRecord(schema, Utf8(id.a), Utf8(uuidStr))
    }

})

@OptIn(InternalSerializationApi::class)
fun valueClassEncodeTest(valueInstance: Any) = stringSpec {
    "encode value class ${valueInstance::class.simpleName}" {
        Avro.default.toAvroCompatibleType(
            valueInstance::class.serializer() as KSerializer<Any>,
            valueInstance
        ) shouldBe valueInstance
    }
    "encode value ${valueInstance::class.valueType.simpleName}" {
        val type = valueInstance::class.valueType
        val actualValue = valueInstance::class.members.filterIsInstance<KProperty1<Any, *>>().first().get(valueInstance)
        Avro.default.toAvroCompatibleType(type.serializer() as KSerializer<Any>, actualValue) shouldBe actualValue
    }
}

private val KClass<*>.valueType: KClass<*>
    get() {
        return this.members.filterIsInstance<KProperty<*>>().first().returnType.classifier as KClass<*>
    }