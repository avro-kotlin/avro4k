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
package com.github.avrokotlin.avro4k.schema

import com.fasterxml.jackson.databind.JsonNode
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.getSchemaNameForUnion
import com.github.avrokotlin.avro4k.possibleSerializationSubclasses
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.capturedKClass
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.Schema.Type.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.min

/**
 * Encapsulate schema-resolution logic in an easy-to-consume representation.
 *
 *
 * This Resolver has been copied from the Apache Avro library and translated to kotlin.
 */
@OptIn(ExperimentalSerializationApi::class)
class Resolver(
    private val avro: Avro
) {
    private val cache = ConcurrentHashMap<SchemaCombination, Action>()

    /**
     * Returns a [Resolver.Action] tree for resolving the writer schema
     * <tt>writer</tt> and the reader schema <tt>reader</tt>.
     *
     *
     * This method walks the reader's and writer's schemas together, generating an
     * appropriate subclass of [Action] to encapsulate the information needed
     * to resolve the corresponding parts of each schema tree. For convenience,
     * every [Action] object has a pointer to the corresponding parts of the
     * reader's and writer's trees being resolved by the action. Each subclass of
     * [Action] has additional information needed for different types of
     * schema, e.g., the [EnumAdjust] subclass has information about
     * re-ordering and deletion of enumeration symbols, while [RecordAdjust]
     * has information about re-ordering and deletion of record fields.
     *
     *
     * Note that aliases are applied to the writer's schema before resolution
     * actually takes place. This means that the <tt>writer</tt> field of the
     * resulting [Action] objects will not be the same schema as provided to
     * this method. However, the <tt>reader</tt> field will be.
     *
     * @return Nested actions for resolving the two
     */
    fun resolve(writer: Schema?, reader: Schema, readerDescriptor: SerialDescriptor): Action {
        return doResolve(Schema.applyAliases(writer, reader), reader, readerDescriptor, HashMap(cache))
    }

    private fun doResolve(
        writerSchema: Schema,
        readerSchema: Schema,
        readerDescriptor: SerialDescriptor,
        seen: MutableMap<SchemaCombination, Action>
    ): Action {
        val cachedValue = seen[SchemaCombination(writerSchema, readerSchema)]
        if (cachedValue != null) {
            return cachedValue
        }
        val writerType = writerSchema.type
        val readerType = readerSchema.type
        if (writerType == UNION) {
            return resolveWriterUnion(writerSchema, readerSchema, readerDescriptor, seen)
        }
        return if (writerType == readerType) {
            when (writerType) {
                NULL,
                BOOLEAN,
                INT,
                LONG,
                FLOAT,
                DOUBLE,
                STRING,
                BYTES -> DoNothing(writerSchema, readerSchema)

                FIXED -> if (writerSchema.fullName != readerSchema.fullName || writerSchema.fixedSize != readerSchema.fixedSize) {
                    throw incompatibleSchemaTypes(writerSchema, readerSchema)
                } else {
                    DoNothing(writerSchema, readerSchema)
                }

                ARRAY -> {
                    val et = doResolve(
                        writerSchema.elementType,
                        readerSchema.elementType,
                        readerDescriptor.getElementDescriptor(0),
                        seen
                    )
                    Container(writerSchema, readerSchema, et)
                }

                MAP -> {
                    val vt = doResolve(
                        writerSchema.valueType,
                        readerSchema.valueType,
                        readerDescriptor.getElementDescriptor(1),
                        seen
                    )
                    Container(writerSchema, readerSchema, vt)
                }

                ENUM -> resolveEnum(writerSchema, readerSchema)
                RECORD -> resolveRecord(writerSchema, readerSchema, readerDescriptor, seen)
                else -> throw IllegalArgumentException("Unknown type for schema: $writerType")
            }
        } else if (readerType == UNION) {
            resolveReaderUnion(writerSchema, readerSchema, readerDescriptor, seen)
        } else {
            resolvePromote(writerSchema, readerSchema)
        }
    }

    /**
     * Resolve an [EnumAdjust].
     */
    fun resolveEnum(w: Schema, r: Schema): EnumAdjust {
        if (w.name != r.name) throw incompatibleSchemaTypes(w, r)
        val wsymbols = w.enumSymbols
        val rsymbols = r.enumSymbols
        val defaultIndex = if (r.enumDefault == null) -1 else rsymbols.indexOf(r.enumDefault)
        val adjustments = IntArray(wsymbols.size)
        for (i in adjustments.indices) {
            var j = rsymbols.indexOf(wsymbols[i])
            if (j < 0) {
                j = defaultIndex
            }
            adjustments[i] = j
        }
        return EnumAdjust(w, r, adjustments)
    }

    fun resolveWriterUnion(
        writeSchema: Schema,
        readSchema: Schema,
        readerDescriptor: SerialDescriptor,
        seen: MutableMap<SchemaCombination, Action>
    ): Action {
        val actions = writeSchema.types.map {
            doResolve(it, readSchema, readerDescriptor, seen)
        }
        return WriterUnion(writeSchema, readSchema, actions)
    }

    /**
     * Returns a [ReaderUnion] action for resolving <tt>w</tt> and <tt>r</tt>,
     * or an [ErrorAction] if there is no branch in the reader that matches
     * the writer.
     *
     * @throws RuntimeException if <tt>r</tt> is not a union schema or <tt>w</tt>
     * *is* a union schema
     */
    fun resolveReaderUnion(
        w: Schema,
        r: Schema,
        readDescriptor: SerialDescriptor,
        seen: MutableMap<SchemaCombination, Action>
    ): ReaderUnion {
        require(w.type != UNION) { "Writer schema is union." }
        val i = firstMatchingBranch(w, r, readDescriptor, seen)
        if (i == -1) throw incompatibleSchemaTypes(w, r)
        val schemaNameToSerialName =
            readDescriptor.possibleSerializationSubclasses(avro.serializersModule).associate {
                Pair(it.getSchemaNameForUnion(avro.nameResolver).fullName, it.serialName)
            }
        val possibleNames =
            r.types.map { if (it.type == Schema.Type.NULL) "null" else schemaNameToSerialName[it.fullName]!! }
        return ReaderUnion(w, r, i, possibleNames, doResolve(w, r.types[i], readDescriptor, seen))

    }

    // Note: This code was taken verbatim from the 1.8.x branch of Apache Avro. It implements
    // a "soft match" algorithm that seems to disagree with the spec. However, in the
    // interest of "bug-for-bug" compatibility, we imported the old algorithm.
    private fun firstMatchingBranch(
        w: Schema,
        r: Schema,
        readerDescriptor: SerialDescriptor,
        seen: MutableMap<SchemaCombination, Action>
    ): Int {
        val writeType = w.type
        // first scan for exact match
        val matchByFullName = r.types.indexOfFirst {
            if (writeType != it.type) {
                false
            } else if (writeType.isNamedType) {
                w.fullName == it.fullName
            } else {
                true
            }
        }
        if (matchByFullName != -1) return matchByFullName
        //No fast match found, now looking for structure match with same name
        val isStructureMatch = { readSchema: Schema ->
            writeType == readSchema.type && !hasMatchError { resolveRecord(w, readSchema, readerDescriptor, seen) }
        }
        val structureMatchSameName = r.types.indexOfFirst {
            w.name == it.name && isStructureMatch.invoke(it)
        }
        if (structureMatchSameName != -1) return structureMatchSameName
        val structureMatch = r.types.indexOfFirst(isStructureMatch)
        if (structureMatch != -1) return structureMatch

        // then scan match via promotion
        return r.types.indexOfFirst { writeType.canBePromotedTo(it.type) }
    }

    private fun hasMatchError(doResolve: () -> Unit): Boolean {
        return try {
            doResolve.invoke()
            false
        } catch (e: Exception) {
            true
        }
    }

    /**
     * Return a promotion.
     *
     * @param w Writer's schema
     * @param r Rearder's schema
     * @return a [Promote] schema if the two schemas are compatible, or
     * [ErrorType.INCOMPATIBLE_SCHEMA_TYPES] if they are not.
     * @throws IllegalArgumentException if *getType()* of the two schemas are
     * not different.
     */
    fun resolvePromote(w: Schema, r: Schema): Promote {
        if (!w.type.canBePromotedTo(r.type)) throw incompatibleSchemaTypes(w, r)
        return Promote(w, r)
    }


    /**
     * Returns a [RecordAdjust] for the two schemas, or an [ErrorAction]
     * if there was a problem resolving. An [ErrorAction] is returned when
     * either the two record-schemas don't have the same name, or if the writer is
     * missing a field for which the reader does not have a default value.
     *
     * @throws RuntimeException if writer and reader schemas are not both records
     */
    fun resolveRecord(
        writeSchema: Schema,
        readSchema: Schema,
        readerDescriptor: SerialDescriptor,
        seen: MutableMap<SchemaCombination, Action>
    ): Action {
        val writeReadPair = SchemaCombination(writeSchema, readSchema)
        val alreadySeenResult = seen[writeReadPair]
        if (alreadySeenResult != null) {
            return alreadySeenResult
        }


        // Current implementation doesn't do this check. To pass regressions tests, we
        // can't either. if (w.getFullName() != null && !
        // w.getFullName().equals(r.getFullName())) { result = new ErrorAction(w, r, d,
        // ErrorType.NAMES_DONT_MATCH); seen.put(wr, result); return result; }
        val writeFields = writeSchema.fields
        val readFields = readSchema.fields
        var firstDefault = 0
        for (writeField in writeFields) {
            // The writeFields that are also in the readschema
            if (readSchema.getField(writeField.name()) != null) {
                ++firstDefault
            }
        }
        val actions = ArrayList<Action>(writeFields.size)
        val reordered = ArrayList<Schema.Field>(readFields.size)
        var result: Action = RecordAdjust(writeSchema, readSchema, actions, reordered, firstDefault)
        seen[writeReadPair] = result // Insert early to handle recursion
        try {
            for (writeField in writeFields) {
                val readField = readSchema.getField(writeField.name())
                if (readField != null) {
                    reordered.add(readField)
                    actions.add(
                        doResolve(
                            writeField.schema(),
                            readField.schema(),
                            readerDescriptor.getElementDescriptor(readField.pos()),
                            seen
                        )
                    )
                } else {
                    actions.add(Skip(writeField.schema()))
                }
            }

            for (readField in readFields) {
                // The field is not in the writeSchema, so we can never read it
                // Use the default value, or throw an error otherwise
                if (writeSchema.getField(readField.name()) == null) {
                    val defaultValue = readField.defaultValue
                    if (defaultValue == null) {
                        throw missingRequiredField(writeSchema, readSchema)
                    } else {
                        //Create default
                        val descriptor = readerDescriptor.getElementDescriptor(readField.pos())
                        println(descriptor.capturedKClass)

                        actions.add(SetDefault(readField.schema(), defaultValue))
                        reordered.add(readField)

                        //TODO("Not yet implemented")
                        //defaults[ridx - firstDefault] = data.getDefaultValue(readField)
                    }
                }
            }
            return result
        } catch (e: SerializationException) {
            seen.remove(writeReadPair)
            throw e
        }

    }

    private fun incompatibleSchemaTypes(w: Schema, r: Schema): SerializationException {
        return SerializationException("The given schema types are not compatible. Found ${w.fullName}, expecting ${r.fullName}.")
    }

    private fun missingRequiredField(w: Schema, r: Schema): SerializationException {
        val fname = r.fields.first { w.getField(it.name()) == null && it.defaultVal() == null }.name()
        return SerializationException("Found ${w.fullName}, expecting ${r.fullName}, missing required field $fname")
    }

    /**
     * An abstract class for an action to be taken to resolve a writer's schema
     * (found in public instance variable <tt>writer</tt>) against a reader's schema
     * (in <tt>reader</tt>). Ordinarily, neither field can be <tt>null</tt>, except
     * that the <tt>reader</tt> field can be <tt>null</tt> in a [Skip], which
     * is used to skip a field in a writer's record that doesn't exist in the
     * reader's (and thus there is no reader schema to resolve to).
     */
    sealed class Action(
        val writer: Schema, val reader: Schema
    )

    /**
     * In this case, there's nothing to be done for resolution: the two schemas are
     * effectively the same. This action will be generated *only* for
     * primitive types and fixed types, and not for any other kind of schema.
     */
    class DoNothing(w: Schema, r: Schema) : Action(w, r)

    /**
     * In this case, the writer's type needs to be promoted to the reader's. These
     * are constructed by [Promote.resolvePromote], which will only construct one
     * when the writer's and reader's schemas are different (ie, no "self
     * promotion"), and when the promotion is one allowed by the Avro spec.
     */
    class Promote(w: Schema, r: Schema) : Action(w, r)

    /**
     * Used for array and map schemas: the public instance variable
     * <tt>elementAction</tt> contains the resolving action needed for the element
     * type of an array or value top of a map.
     */
    class Container(w: Schema, r: Schema, val elementAction: Action) : Action(w, r)

    /**
     * Contains information needed to resolve enumerations. When resolving enums,
     * adjustments need to be made in two scenarios: the index for an enum symbol
     * might be different in the reader or writer, or the reader might not have a
     * symbol that was written out for the writer (which is an error, but one we can
     * only detect when decoding data).
     *
     *
     * These adjustments are reflected in the instance variable
     * <tt>adjustments</tt>. For the symbol with index <tt>i</tt> in the writer's
     * enum definition, <tt>adjustments[i]</tt> -- and integer -- contains the
     * adjustment for that symbol. If the integer is positive, then reader also has
     * the symbol and the integer is its index in the reader's schema. If
     * <tt>adjustment[i]</tt> is negative, then the reader does *not* have
     * the corresponding symbol (which is the error case).
     *
     *
     * Sometimes there's no adjustments needed: all symbols in the reader have the
     * same index in the reader's and writer's schema. This is a common case, and it
     * allows for some optimization. To signal that this is the case,
     * <tt>noAdjustmentsNeeded</tt> is set to true.
     */
    class EnumAdjust(
        w: Schema, r: Schema, val adjustments: IntArray
    ) : Action(w, r) {

        val noAdjustmentsNeeded: Boolean

        init {
            var noAdj: Boolean
            val rsymCount = r.enumSymbols.size
            val count = min(rsymCount.toDouble(), adjustments.size.toDouble()).toInt()
            noAdj = adjustments.size <= rsymCount
            var i = 0
            while (noAdj && i < count) {
                noAdj = i == adjustments[i]
                i++
            }
            noAdjustmentsNeeded = noAdj
        }
    }

    /**
     * This only appears inside [RecordAdjust.fieldActions], i.e., the actions
     * for adjusting the fields of a record. This action indicates that the writer's
     * schema has a field that the reader's does *not* have, and thus the
     * field should be skipped. Since there is no corresponding reader's schema for
     * the writer's in this case, the [Action.reader] field is <tt>null</tt>
     * for this subclass.
     */
    class Skip(w: Schema) : Action(w, w)

    /**
     * Instructions for resolving two record schemas. Includes instructions on how
     * to recursively resolve each field, an indication of when to skip (writer
     * fields), plus information about which reader fields should be populated by
     * defaults (because the writer doesn't have corresponding fields).
     */
    class RecordAdjust(
        w: Schema, r: Schema,
        /**
         * An action for each field of the writer. If the corresponding field is to be
         * skipped during reading, then this will contain a [Skip]. For fields to
         * be read into the reading datum, will contain a regular action for resolving
         * the writer/reader schemas of the matching fields.
         */
        val fieldActions: MutableList<Action>,
        /**
         * Contains (all of) the reader's fields. The first *n* of these are the
         * fields that will be read from the writer: these *n* are in the order
         * dictated by writer's schema. The remaining *m* fields will be read from
         * default values (actions for these default values are found in
         * [defaults].
         */
        val readerOrder: MutableList<Schema.Field>,
        /**
         * Pointer into [RecordAdjust.readerOrder] of the first reader field whose
         * value comes from a default value. Set to length of
         * [RecordAdjust.readerOrder] if there are none.
         */
        val firstDefault: Int
    ) : Action(w, r) {
        /**
         * Returns true iff `i == readerOrder[i].pos()` for all
         * indices `i`. Which is to say: the order of the reader's fields is
         * the same in both the reader's and writer's schema.
         */
        val noReorder: Boolean by lazy {
            var result = true
            var i = 0
            while (result && i < readerOrder.size) {
                result = result && (i == readerOrder[i].pos())
                i++
            }
            result
        }
    }

    /**
     * In this case, the writer was a union. There are two subcases here:
     *
     *
     * The <tt>actions</tt> list holds the resolutions
     * of each branch of the writer against the corresponding branch of the reader
     * (which will result in no material resolution work, because the branches will
     * be equivalent). If they reader is not a union or is a different union, then
     * <tt>unionEquiv</tt> is false and the <tt>actions</tt> list holds the
     * resolution of each of the writer's branches against the entire schema of the
     * reader (if the reader is a union, that will result in ReaderUnion actions).
     */
    class WriterUnion(
        w: Schema, r: Schema, val actions: List<Action>
    ) : Action(w, r)

    inner class SetDefault(r: Schema, private val jsonDefault: String) : Action(r, r) {
        val isDefaultNull = jsonDefault == "null"
        val decodedDefaultValue: Any? = when (r.type) {
            Schema.Type.FIXED,
            Schema.Type.BYTES -> ByteArraySerializer()

            Schema.Type.STRING -> String.serializer()
            Schema.Type.INT -> Int.serializer()
            Schema.Type.LONG -> Long.serializer()
            Schema.Type.BOOLEAN -> Boolean.serializer()
            Schema.Type.FLOAT -> Float.serializer()
            Schema.Type.DOUBLE -> Double.serializer()
            else -> null
        }?.let { avro.decodeFromJsonString(jsonDefault, it, r, r) }

        fun <T> decodeDefault(deserializationStrategy: DeserializationStrategy<T>): T {
            @Suppress("UNCHECKED_CAST")
            if (decodedDefaultValue != null) return decodedDefaultValue as T
            return avro.decodeFromJsonString(jsonDefault, deserializationStrategy, reader, reader)
        }

        inline fun <reified T> decodeDefault(): T {
            if (decodedDefaultValue != null) return decodedDefaultValue as T
            return decodeDefault(serializer())
        }
    }


    /**
     * In this case, the reader is a union and the writer is not. For this case, we
     * need to pick the first branch of the reader that matches the writer and
     * pretend to the reader that the index of this branch was found in the writer's
     * data stream.
     *
     *
     * To support this case, the [ReaderUnion] object has two (public) fields:
     * <tt>firstMatch</tt> gives the index of the first matching branch in the
     * reader's schema, and <tt>actualResolution</tt> is the [Action] that
     * resolves the writer's schema with the schema found in the <tt>firstMatch</tt>
     * branch of the reader's schema.
     */
    class ReaderUnion(
        w: Schema,
        r: Schema,
        val firstMatch: Int,
        val readerSerialNames: List<String>,
        val actualAction: Action
    ) : Action(w, r)

    private val Schema.Type.isNamedType: Boolean
        get() = this == Schema.Type.FIXED || this == Schema.Type.RECORD || this == Schema.Type.ENUM

    private fun Schema.Type.canBePromotedTo(otherType: Schema.Type): Boolean {
        return when (this) {
            INT -> when (otherType) {
                LONG, DOUBLE, FLOAT -> true
                else -> false
            }

            LONG -> when (otherType) {
                DOUBLE, FLOAT -> true
                else -> false
            }

            FLOAT -> otherType == DOUBLE
            STRING -> otherType == BYTES
            BYTES -> otherType == STRING
            else -> false
        }
    }
}

private val defaultValueField = Schema.Field::class.java.getDeclaredField("defaultValue")
private val Schema.Field.defaultValue: String?
    get() {
        try {
            defaultValueField.isAccessible = true
            val jsonNode: JsonNode = defaultValueField.get(this) as? JsonNode ?: return null
            return jsonNode.toString()
        } finally {
            defaultValueField.isAccessible = false
        }
    }

class SchemaCombination(
    val writeSchema: Schema,
    val readSchema: Schema
) {
    private val hashCode: Int by lazy {
        //Take the fullname for speed
        writeSchema.fullName.hashCode() + readSchema.fullName.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SchemaCombination) return false

        if (writeSchema !== other.writeSchema) return false
        if (readSchema !== other.readSchema) return false

        return true
    }

    override fun hashCode(): Int = hashCode
}

