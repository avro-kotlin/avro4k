package com.github.avrokotlin.avro4k.io

/**
 * An [AvroOutputStream] will write instances of T to an underlying
 * representation.
 *
 * There are three implementations of this stream
 *  - a Data stream,
 *  - a Binary stream
 *  - a Json stream
 *
 * See the methods on the companion object to create instances of each
 * of these types of stream.
 */
interface AvroOutputStream<T> : AutoCloseable {
    fun flush()

    fun fSync()

    fun write(t: T): AvroOutputStream<T>

    fun write(ts: List<T>): AvroOutputStream<T> {
        ts.forEach { write(it) }
        return this
    }
}