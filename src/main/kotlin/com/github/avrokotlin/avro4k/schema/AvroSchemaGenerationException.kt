package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.SerializationException

class AvroSchemaGenerationException(message: String) : SerializationException(message)