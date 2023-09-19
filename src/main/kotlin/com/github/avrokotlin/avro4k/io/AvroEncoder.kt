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

import kotlinx.io.Buffer
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema


/**
 * Low-level support for serializing Avro values.
 *
 * This class has two types of methods. One type of methods support the writing
 * of leaf values (for example, [.writeLong] and [.writeString]).
 * These methods have analogs in [AvroDecoder].
 *
 *
 * The other type of methods support the writing of maps and arrays. These
 * methods are [.writeArrayStart], [.startItem], and
 * [.writeArrayEnd] (and similar methods for maps). Some implementations
 * of [AvroEncoder] handle the buffering required to break large maps and
 * arrays into blocks, which is necessary for applications that want to do
 * streaming. (See [.writeArrayStart] for details on these methods.)
 *
 * This code has been derived from `org.apache.avro.io.Encoder` of the avro project
 * and converted to kotlin multiplatform.
 */
abstract class AvroEncoder {
    /**
     * "Writes" a null value. (Doesn't actually write anything, but advances the
     * state of the parser if this class is stateful.)
     */
    abstract fun writeNull()

    /**
     * Write a boolean value.
     */
    abstract fun writeBoolean(b: Boolean)

    /**
     * Writes a 32-bit integer.
     */
    abstract fun writeInt(n: Int)

    /**
     * Write a 64-bit integer.
     */
    abstract fun writeLong(n: Long)

    /**
     * Write a float.
     */
    abstract fun writeFloat(f: Float)

    /**
     * Write a double.
     */
    abstract fun writeDouble(d: Double)

    /**
     * Write a Unicode character string.
     */
    abstract fun writeString(str: CharSequence);

    /**
     * Write a byte string.
     */
    abstract fun writeBytes(bytes: Buffer)

    /**
     * Writes a byte string. Equivalent to
     * <tt>writeBytes(bytes, 0, bytes.length)</tt>
     */
    abstract fun writeBytes(bytes: ByteArray)

    /**
     * Writes a fixed size binary object.
     *
     * @param bytes The contents to write
     * @param len   The number of bytes to write.
     */
    abstract fun writeFixed(bytes: Buffer)

    /**
     * A shorthand for <tt>writeFixed(bytes, 0, bytes.length)</tt>
     *
     * @param bytes
     */
    abstract fun writeFixed(bytes: ByteArray)

    /**
     * Writes an enumeration.
     *
     * @param e
     */
    abstract fun writeEnum(e: Int)

    /**
     * Call this method to start writing an array.
     *
     * When starting to serialize an array, call [.writeArrayStart]. Then,
     * before writing any data for any item call [.setItemCount] followed by a
     * sequence of [.startItem] and the item itself. The number of
     * [.startItem] should match the number specified in
     * [.setItemCount]. When actually writing the data of the item, you can
     * call any [AvroEncoder] method (e.g., [.writeLong]). When all items of
     * the array have been written, call [.writeArrayEnd].
     *
     * As an example, let's say you want to write an array of records, the record
     * consisting of an Long field and a Boolean field. Your code would look
     * something like this:
     *
     * <pre>
     * out.writeArrayStart();
     * out.setItemCount(list.size());
     * for (Record r : list) {
     * out.startItem();
     * out.writeLong(r.longField);
     * out.writeBoolean(r.boolField);
     * }
     * out.writeArrayEnd();
    </pre> *
     *
     */
    abstract fun writeArrayStart(size: Int)
    
    /**
     * Start a new item of an array or map. See [.writeArrayStart] for usage
     * information.
     *
     */
    abstract fun startItem()

    /**
     * Call this method to finish writing an array. See [.writeArrayStart] for
     * usage information.
     */
    abstract fun writeArrayEnd()

    /**
     * Call this to start a new map. See [.writeArrayStart] for details on
     * usage.
     *
     * As an example of usage, let's say you want to write a map of records, the
     * record consisting of an Long field and a Boolean field. Your code would look
     * something like this:
     *
     * <pre>
     * out.writeMapStart();
     * out.setItemCount(list.size());
     * for (Map.Entry<String></String>, Record> entry : map.entrySet()) {
     * out.startItem();
     * out.writeString(entry.getKey());
     * out.writeLong(entry.getValue().longField);
     * out.writeBoolean(entry.getValue().boolField);
     * }
     * out.writeMapEnd();
    </pre> *
     */
    abstract fun writeMapStart(size: Int)

    /**
     * Call this method to terminate the inner-most, currently-opened map. See
     * [.writeArrayStart] for more details.
     */
    abstract fun writeMapEnd()

    /**
     * Call this method to write the tag of a union.
     *
     * As an example of usage, let's say you want to write a union, whose second
     * branch is a record consisting of an Long field and a Boolean field. Your code
     * would look something like this:
     *
     * <pre>
     * out.writeIndex(1);
     * out.writeLong(record.longField);
     * out.writeBoolean(record.boolField);
    </pre> *
     */
    abstract fun writeIndex(unionIndex: Int)

    abstract fun flush()

    fun writeFixedWithPadding(bytes: ByteArray, fixedSize: Int) {
        val actualSize = bytes.size
        val padding = fixedSize - actualSize
        if(padding < 0) {
            throw AvroRuntimeException("Cannot write string with ${actualSize} bytes to fixed type of size ${fixedSize}")
        } else if(padding > 0) {
            val paddingArr = ByteArray(padding)
            writeFixed(paddingArr)
            if(actualSize > 0) {
                writeFixed(bytes)
            }
        } else {
            writeFixed(bytes)
        }
    }
    fun writeString(schema: Schema, t: String) {
        when (schema.type) {
            Schema.Type.FIXED -> {
                writeFixedWithPadding(t.encodeToByteArray(), schema.fixedSize)
            }

            Schema.Type.BYTES -> {
                writeBytes(t.encodeToByteArray())
            }

            else -> writeString(t)
        }
    }

    fun writeUnionSchema(unionSchema: Schema, actualSchema: Schema) {
        writeUnionSchema(unionSchema, unionSchema.types.indexOf(actualSchema))
    }

    abstract fun writeUnionSchema(unionSchema: Schema, indexOfActualSchema: Int)
    fun writeEnum(schema: Schema, enumDescription: SerialDescriptor, ordinal: Int) {
        // the schema provided will be a union, so we should extract the correct schema
        val symbol = enumDescription.getElementName(ordinal)
        writeInt(schema.enumSymbols.indexOf(symbol))
    }

    abstract fun writeByte(value: Byte)
}
