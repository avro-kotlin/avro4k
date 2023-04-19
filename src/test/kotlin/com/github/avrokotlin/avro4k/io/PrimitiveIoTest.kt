package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.schema.*
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.serializer

class PrimitiveIoTest : StringSpec({

    "read / write Boolean value class" {
        writeRead(BooleanWrapper(true), BooleanWrapper.serializer())
    }
    "read / write Boolean type" {
        writeRead(true, Boolean.serializer())
    }
    "read / write Byte value class" {
        writeRead(ByteWrapper(0x01), ByteWrapper.serializer())        
    }
    "read / write Byte type" {
        writeRead(0x01, Byte.serializer())        
    }
    "read / write Short value class" {
        writeRead(ShortWrapper(2), ShortWrapper.serializer())        
    }
    "read / write Short type" {
        writeRead(2, Short.serializer())        
    }
    "read / write Int value class" {
        writeRead(IntWrapper(3), IntWrapper.serializer())
    }
    "read / write Int type" {
        writeRead(3, Int.serializer())        
    }
    "read / write Long value class" {
        writeRead(LongWrapper(9L), LongWrapper.serializer())
    }
    "read / write Long type" {
        writeRead(10L, Long.serializer())
    }
    "read / write Float value class" {
        writeRead(FloatWrapper(6.5f), FloatWrapper.serializer())
    }
    "read / write Float type" {
        writeRead(9.9f, Float.serializer())        
    }
    "read / write Double value class" {        
        writeRead(DoubleWrapper(0.23455), DoubleWrapper.serializer())
    }
    "read / write Double type" {
        writeRead(0.323, Double.serializer())
    }
    "read / write ByteArray value class" {
        writeRead(ByteArrayWrapper(byteArrayOf(0x01, 0x05)), ByteArrayWrapper.serializer())        
    }
    "read / write ByteArray type" {
        writeRead(byteArrayOf(0x05, 0x06), ByteArraySerializer())        
    }
    "read / write String value class" {
        writeRead(StringWrapper("bllla"), StringWrapper.serializer())
    }
    "read / write String type" {
        writeRead("bassss", String.serializer())
    }
})