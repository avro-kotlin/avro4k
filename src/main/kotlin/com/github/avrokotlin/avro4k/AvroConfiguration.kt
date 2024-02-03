package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.FieldNamingStrategy
import com.github.avrokotlin.avro4k.schema.RecordName
import com.github.avrokotlin.avro4k.schema.RecordNamingStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import java.util.concurrent.ConcurrentHashMap

data class AvroConfiguration(
    /**
     * The naming strategy to use for record's name and namespace. Also applied for fixed and enum types.
     *
     * Default: [RecordNamingStrategy.Builtins.FullyQualified]
     */
    val recordNamingStrategy: RecordNamingStrategy = RecordNamingStrategy.Builtins.FullyQualified,
    /**
     * The naming strategy to use for field's name.
     *
     * Default: [FieldNamingStrategy.Builtins.NoOp]
     */
    val fieldNamingStrategy: FieldNamingStrategy = FieldNamingStrategy.Builtins.NoOp,
    /**
     * By default, during decoding, any missing value for a nullable field without default `null` value (e.g. `val field: Type?` without `= null`) is failing.
     * When set to `true`, the nullable fields that haven't any default value are set as null if the value is missing. It also adds `"default": null` to those fields when generating schema using avro4k.
     */
    val implicitNulls: Boolean = false,
    /**
     * Enable caching of resolved names.
     *
     * Default: `true`
     */
    val namingCacheEnabled: Boolean = true,
)

class AvroInternalConfiguration private constructor(
    val recordNamingStrategy: RecordNamingStrategy,
    val fieldNamingStrategy: FieldNamingStrategy,
    val implicitNulls: Boolean,
) {
    constructor(configuration: AvroConfiguration) : this(
        recordNamingStrategy = configuration.recordNamingStrategy.cachedIfNecessary(configuration.namingCacheEnabled),
        fieldNamingStrategy = configuration.fieldNamingStrategy.cachedIfNecessary(configuration.namingCacheEnabled),
        implicitNulls = configuration.implicitNulls
    )
}

internal fun RecordNamingStrategy.cachedIfNecessary(cacheEnabled: Boolean): RecordNamingStrategy =
    if (!cacheEnabled) {
        this
    } else {
        object : RecordNamingStrategy {
            private val cache = ConcurrentHashMap<SerialDescriptor, RecordName>()

            override fun resolve(
                descriptor: SerialDescriptor,
                serialName: String,
            ): RecordName =
                cache.getOrPut(descriptor) {
                    this@cachedIfNecessary.resolve(descriptor, serialName)
                }
        }
    }

internal fun FieldNamingStrategy.cachedIfNecessary(cacheEnabled: Boolean): FieldNamingStrategy =
    if (!cacheEnabled) {
        this
    } else {
        object : FieldNamingStrategy {
            private val cache = ConcurrentHashMap<Pair<SerialDescriptor, Int>, String>()

            override fun resolve(
                descriptor: SerialDescriptor,
                elementIndex: Int,
                serialName: String,
            ): String =
                cache.getOrPut(descriptor to elementIndex) {
                    this@cachedIfNecessary.resolve(descriptor, elementIndex, serialName)
                }
        }
    }