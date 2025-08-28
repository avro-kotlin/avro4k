package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.schema
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import io.kotest.matchers.types.beOfType
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

class ReflectKSerializerTest : StringSpec() {
    init {
        "ENUM should be deserialized as concrete class" {
            val schema = Avro.schema<TestEnum>()

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), byteArrayOf(2)) shouldBe TestEnum.B
        }
        "ENUM should be deserialized as concrete class based on the alias when registered in the serializers module" {
            val schema = Schema.createEnum("theEnum", null, null, listOf("A", "B", "C"))

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), byteArrayOf(2)) shouldBe GenericData.EnumSymbol(schema, "B")

            Avro {
                serializersModule =
                    SerializersModule {
                        contextual(TestEnum::class, TestEnum.serializer())
                    }
            }.decodeFromByteArray(schema, ReflectKSerializer(), byteArrayOf(2)) shouldBe TestEnum.B
        }
        "ENUM should be deserialized as concrete class based on the custom serial name when registered in the serializers module" {
            val schema = Schema.createEnum("theTestEnumCustomSerialName", null, null, listOf("A", "B", "C"))

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), byteArrayOf(2)) shouldBe GenericData.EnumSymbol(schema, "B")

            Avro {
                serializersModule =
                    SerializersModule {
                        contextual(TestEnumCustomSerialName::class, TestEnumCustomSerialName.serializer())
                    }
            }.decodeFromByteArray(schema, ReflectKSerializer(), byteArrayOf(2)) shouldBe TestEnumCustomSerialName.B
        }
        "ENUM should be deserialized as GenericEnumSymbol if the schema's full-name doesn't exist as a class" {
            val schema = Schema.createEnum("theEnum", null, null, listOf("A", "B", "C"))

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), byteArrayOf(2)) shouldBe GenericData.EnumSymbol(schema, "B")
        }
        "MAP should be deserialized as a Map of excepted key type when having java-key-class property" {
            val schema =
                Schema.createMap(Schema.create(Schema.Type.STRING)).also {
                    it.addProp("java-key-class", Int::class.javaObjectType.name)
                }
            val bytes = Avro.encodeToByteArray(schema, ReflectKSerializer(), mapOf("1" to "A", "2" to "B"))

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), bytes) shouldBe mapOf(1 to "A", 2 to "B")
        }
        "ARRAY should be deserialized as a List of excepted item type when having java-element-class property" {
            val schema =
                Schema.createArray(Schema.create(Schema.Type.STRING)).also {
                    it.addProp("java-element-class", Int::class.javaObjectType.name)
                }
            val bytes = Avro.encodeToByteArray(schema, ReflectKSerializer(), listOf("1", "2", "3"))

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), bytes) shouldBe listOf(1, 2, 3)
        }
        "deserializing type with java-class should return the class instance specified in the property" {
            val schema =
                Schema.create(Schema.Type.STRING).also {
                    it.addProp("java-class", CustomString::class.java.name)
                }
            val bytes = Avro.encodeToByteArray(schema, "Hello")

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), bytes) shouldBe CustomString("Hello")
        }
        "deserializing type with an unknown java-class should return the natural type" {
            val schema =
                Schema.create(Schema.Type.STRING).also {
                    it.addProp("java-class", "unknown.ClassName")
                }
            val bytes = Avro.encodeToByteArray(schema, "Hello")

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), bytes) shouldBe "Hello"
        }
        "FIXED should be deserialized as concrete class" {
            val schema = Schema.createFixed(CustomFixed::class.qualifiedName, null, null, 3)
            val bytes = byteArrayOf(1, 2, 3)

            val decoded = Avro.decodeFromByteArray(schema, ReflectKSerializer(), bytes)
            decoded should beOfType<CustomFixed>()
            (decoded as CustomFixed).value shouldBe bytes
        }
        "FIXED should be deserialized as concrete class based on the alias when registered in the serializers module" {
            val schema = Schema.createFixed("theFixed", null, null, 3)
            val bytes = byteArrayOf(1, 2, 3)

            val decoded =
                Avro {
                    serializersModule =
                        SerializersModule {
                            contextual(CustomFixed::class, CustomFixed.serializer())
                        }
                }.decodeFromByteArray(schema, ReflectKSerializer(), bytes)
            decoded should beOfType<CustomFixed>()
            (decoded as CustomFixed).value shouldBe bytes
        }
        "FIXED should be deserialized as GenericFixed if the schema's full-name doesn't exist as a class" {
            val schema = Schema.createFixed("theFixed", null, null, 3)
            val bytes = byteArrayOf(1, 2, 3)

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), bytes) shouldBe GenericData.Fixed(schema, byteArrayOf(1, 2, 3))
        }
        "RECORD should be deserialized as concrete class" {
            val schema = Avro.schema<TheRecord>()
            val bytes = Avro.encodeToByteArray(schema, TheRecord("Hello", null))

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), bytes) shouldBe TheRecord("Hello", null)
        }
        "RECORD should be deserialized as concrete class based on the alias when registered in the serializers module" {
            val schema = Avro.schema<TheRecord>().copy(name = "ns.theRecord", namespace = "")
            val bytes = Avro.encodeToByteArray(schema, TheRecord("Hello", null))

            // not specifying the configured Avro instance should fallback on deserializing as GenericRecord
            Avro.decodeFromByteArray<Any>(schema, ReflectKSerializer(), bytes) should beInstanceOf<GenericRecord>()

            val avro =
                Avro {
                    serializersModule =
                        SerializersModule {
                            contextual(TheRecord::class, TheRecord.serializer())
                        }
                }
            avro.decodeFromByteArray(schema, ReflectKSerializer(), bytes) shouldBe TheRecord("Hello", null)
        }
        "RECORD should be deserialized as GenericRecord if the schema's full-name doesn't exist as a class" {
            val schema =
                SchemaBuilder.record("theRecord")
                    .fields()
                    .requiredString("field")
                    .endRecord()
            val value =
                GenericData.Record(schema).apply {
                    put("field", "Hello")
                }
            val bytes = Avro.encodeToByteArray(schema, ReflectKSerializer(), value)

            Avro.decodeFromByteArray(schema, ReflectKSerializer(), bytes) shouldBe value
        }
    }
}

@JvmInline
@Serializable
private value class CustomString(val value: String)

@JvmInline
@Serializable
@AvroAlias("theFixed")
private value class CustomFixed(val value: ByteArray)

@Serializable
@AvroAlias("theEnum")
private enum class TestEnum {
    A,
    B,
    C,
}

@Serializable
@SerialName("theTestEnumCustomSerialName")
private enum class TestEnumCustomSerialName {
    A,
    B,
    C,
}

@Serializable
@AvroAlias("ns.theRecord")
private data class TheRecord(val field1: String, val field2: Int?)