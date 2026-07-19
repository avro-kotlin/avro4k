package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.builtins.serializer
import java.io.File
import java.net.URL
import java.net.URLClassLoader

internal class OptionalKotlinInstantClasspathTest : StringSpec({
    "loading Avro, generating a schema, and encoding data should not fail if kotlin.time.Instant is missing" {
        ClassLoaderWithoutKotlinInstant(testRuntimeClasspathUrls()).use { classLoader ->
            val probeClass = classLoader.loadClass("com.github.avrokotlin.avro4k.OptionalKotlinInstantRuntimeProbe")
            val schema = probeClass.getMethod("loadDefaultAndGenerateStringSchema").invoke(null)
            val decoded = probeClass.getMethod("encodeAndDecodeInt").invoke(null)

            schema shouldBe "\"string\""
            decoded shouldBe 42
        }
    }
})

public object OptionalKotlinInstantRuntimeProbe {
    @JvmStatic
    public fun loadDefaultAndGenerateStringSchema(): String = Avro.schema<String>().toString()

    @JvmStatic
    public fun encodeAndDecodeInt(): Int {
        val serializer = Int.serializer()
        val schema = Avro.schema(serializer.descriptor)
        val bytes = Avro.encodeToByteArray(schema, serializer, 42)
        return Avro.decodeFromByteArray(schema, serializer, bytes)
    }
}

private fun testRuntimeClasspathUrls(): Array<URL> {
    val contextClassLoader = Thread.currentThread().contextClassLoader
    if (contextClassLoader is URLClassLoader) {
        return contextClassLoader.urLs
    }
    return System.getProperty("java.class.path")
        .split(File.pathSeparator)
        .map(::File)
        .filter(File::exists)
        .map { it.toURI().toURL() }
        .toTypedArray()
}

private class ClassLoaderWithoutKotlinInstant(urls: Array<URL>) :
    URLClassLoader(urls, ClassLoader.getPlatformClassLoader()) {
    override fun loadClass(
        name: String,
        resolve: Boolean,
    ): Class<*> {
        if (name == "kotlin.time.Instant" || name.startsWith("kotlin.time.Instant$")) {
            throw ClassNotFoundException(name)
        }
        synchronized(getClassLoadingLock(name)) {
            findLoadedClass(name)?.let {
                if (resolve) {
                    resolveClass(it)
                }
                return it
            }
            return super.loadClass(name, resolve)
        }
    }
}