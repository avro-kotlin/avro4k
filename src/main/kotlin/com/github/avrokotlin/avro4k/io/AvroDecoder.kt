/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.avrokotlin.avro4k.io

/**
 * Low-level support for de-serializing Avro values.
 *
 * This code has been derived from `org.apache.avro.io.Decoder` of the avro project
 * and converted to kotlin multiplatform.
 */
abstract class AvroDecoder {
    /**
     * "Reads" a null value. (Doesn't actually read anything, but advances the state
     * of the parser if the implementation is stateful.)
     */
    abstract fun readNull()

    /**
     * Reads a boolean value written by [AvroEncoder.writeBoolean].
     */
    abstract fun readBoolean(): Boolean

    /**
     * Reads an integer written by [AvroEncoder.writeInt].
     */
    abstract fun readInt(): Int

    /**
     * Reads a long written by [AvroEncoder.writeLong].
     */
    abstract fun readLong(): Long

    /**
     * Reads a float written by [AvroEncoder.writeFloat].
     */
    abstract fun readFloat(): Float

    /**
     * Reads a double written by [AvroEncoder.writeDouble].
     */
    abstract fun readDouble(): Double

    /**
     * Reads a char-string written by [AvroEncoder.writeString].
     */
    abstract fun readString(): String

    /**
     * Discards a char-string written by [AvroEncoder.writeString].
     */
    abstract fun skipString()

    /**
     * Reads a byte-string written by [AvroEncoder.writeBytes].
     */
    abstract fun readBytes(): ByteArray

    /**
     * Discards a byte-string written by [AvroEncoder.writeBytes].
     */
    abstract fun skipBytes()

    /**
     * Reads fixed sized binary object.
     *
     * @param bytes  The buffer to store the contents being read.
     * @param start  The position where the data needs to be written.
     * @param length The size of the binary object.
     */
    abstract fun readFixed(bytes: ByteArray, start: Int, length: Int)

    /**
     * A shorthand for <tt>readFixed(bytes, 0, bytes.length)</tt>.
     */
    fun readFixed(bytes: ByteArray) {
        readFixed(bytes, 0, bytes.size)
    }

    /**
     * Discards fixed sized binary object.
     *
     * @param length The size of the binary object to be skipped.
     */
    abstract fun skipFixed(length: Int)

    /**
     * Reads an enumeration.
     *
     * @return The enumeration's value.
     */
    abstract fun readEnum(): Int

    /**
     * Reads and returns the size of the first block of an array. If this method
     * returns non-zero, then the caller should read the indicated number of items,
     * and then call [.arrayNext] to find out the number of items in the next
     * block. The typical pattern for consuming an array looks like:
     *
     * <pre>
     * for(long i = in.readArrayStart(); i != 0; i = in.arrayNext()) {
     * for (long j = 0; j < i; j++) {
     * read next element of the array;
     * }
     * }
     * </pre>
     */
    abstract fun readArrayStart(): Long

    /**
     * Processes the next block of an array and returns the number of items in the
     * block and lets the caller read those items.
     */
    abstract fun arrayNext(): Long

    /**
     * Used for quickly skipping through an array. Note you can either skip the
     * entire array, or read the entire array (with [.readArrayStart]), but
     * you can't mix the two on the same array.
     *
     * This method will skip through as many items as it can, all of them if
     * possible. It will return zero if there are no more items to skip through, or
     * an item count if it needs the client's help in skipping. The typical usage
     * pattern is:
     *
     * <pre>
     * for(long i = in.skipArray(); i != 0; i = i.skipArray()) {
     * for (long j = 0; j < i; j++) {
     * read and discard the next element of the array;
     * }
     * }
     * </pre>
     *
     * Note that this method can automatically skip through items if a byte-count is
     * found in the underlying data, or if a schema has been provided to the
     * implementation, but otherwise the client will have to skip through items
     * itself.
     */
    abstract fun skipArray(): Long

    /**
     * Reads and returns the size of the next block of map-entries. Similar to
     * [.readArrayStart].
     *
     * As an example, let's say you want to read a map of records, the record
     * consisting of a Long field and a Boolean field. Your code would look
     * something like this:
     *
     * <pre>
     * Map<String></String>, Record> m = new HashMap<String></String>, Record>();
     * Record reuse = new Record();
     * for (long i = in.readMapStart(); i != 0; i = in.readMapNext()) {
     * for (long j = 0; j < i; j++) {
     * String key = in.readString();
     * reuse.intField = in.readInt();
     * reuse.boolField = in.readBoolean();
     * m.put(key, reuse);
     * }
     * }
     * </pre>
     */
    abstract fun readMapStart(): Long

    /**
     * Processes the next block of map entries and returns the count of them.
     * Similar to [.arrayNext]. See [.readMapStart] for details.
     */
    abstract fun mapNext(): Long

    /**
     * Support for quickly skipping through a map similar to [.skipArray].
     *
     * As an example, let's say you want to skip a map of records, the record
     * consisting of an Long field and a Boolean field. Your code would look
     * something like this:
     *
     * <pre>
     * for (long i = in.skipMap(); i != 0; i = in.skipMap()) {
     * for (long j = 0; j < i; j++) {
     * in.skipString(); // Discard key
     * in.readInt(); // Discard int-field of value
     * in.readBoolean(); // Discard boolean-field of value
     * }
     * }
     * </pre>
     */
    abstract fun skipMap(): Long

    /**
     * Reads the tag of a union written by [AvroEncoder.writeIndex].
     */
    abstract fun readIndex(): Int
}
