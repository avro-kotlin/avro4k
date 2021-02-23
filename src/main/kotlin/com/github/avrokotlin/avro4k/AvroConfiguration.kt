package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.DefaultNamingStrategy
import com.github.avrokotlin.avro4k.schema.NamingStrategy

data class AvroConfiguration(val namingStrategy: NamingStrategy = DefaultNamingStrategy)
