package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import java.io.File
import java.net.URL
import java.net.URLClassLoader

internal class OptionalKotlinInstantClasspathTest : StringSpec({
    "load Avro and generate schemas when kotlin.time.Instant is absent but unused" {
        OptionalKotlinInstantBlockingClassLoader(testRuntimeClasspathUrls()).use { classLoader ->
            val probeClass = classLoader.loadClass("com.github.avrokotlin.avro4k.OptionalKotlinInstantRuntimeProbe")
            val schema = probeClass.getMethod("loadDefaultAndGenerateStringSchema").invoke(null)

            schema shouldBe "\"string\""
        }
    }
})

public object OptionalKotlinInstantRuntimeProbe {
    @JvmStatic
    public fun loadDefaultAndGenerateStringSchema(): String = Avro.schema<String>().toString()
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

private class OptionalKotlinInstantBlockingClassLoader(urls: Array<URL>) :
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