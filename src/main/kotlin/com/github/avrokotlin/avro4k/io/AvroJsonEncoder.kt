package com.github.avrokotlin.avro4k.io

import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.io.asOutputStream
import kotlinx.io.readByteArray
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.*
import org.apache.avro.Schema

val json = Json { prettyPrint = false }
val stringSchema = Schema.create(Schema.Type.STRING)

        /**
 * An [AvroEncoder] for Avro's JSON encoding.
 *
 * This encoder is not optimized for speed. Prefer the [AvroBinaryEncoder].
 */
@OptIn(ExperimentalSerializationApi::class)
class AvroJsonEncoder(val out: Sink) : AvroEncoder() {
    lateinit var currentContainer: JsonElementEncoder
    val containerStack = ArrayDeque<JsonElementEncoder>()

    override fun configure(schema: Schema) {
        currentContainer = JsonRootEncoder(schema)
        containerStack.clear()
    }

    override fun writeNull() = addItem(JsonNull)

    override fun writeBoolean(b: Boolean) = addItem(JsonPrimitive(b))

    override fun writeInt(n: Int) = addItem(JsonPrimitive(n))

    override fun writeLong(n: Long) = addItem(JsonPrimitive(n))

    override fun writeFloat(f: Float) = addItem(JsonPrimitive(f))

    override fun writeDouble(d: Double) = addItem(JsonPrimitive(d))

    override fun writeByte(value: Byte) = addItem(JsonPrimitive(value))

    override fun writeString(cs: CharSequence) = addItem(JsonPrimitive(cs.toString()))

    override fun writeBytes(bytes: Buffer) = writeBytes(bytes.readByteArray())
    override fun writeBytes(bytes: ByteArray) = addItem(JsonPrimitive(bytes.toJsonString()))

    override fun writeFixed(bytes: Buffer) = writeBytes(bytes)

    override fun writeFixed(bytes: ByteArray) = writeBytes(bytes)

    override fun writeEnum(e: Int) {
        determineCurrentContainer()
        writeString(currentContainer.elementSchema.enumSymbols[e])
    } 

    override fun writeArrayStart(size: Int) {
        determineCurrentContainer()
        push(JsonArrayEncoder(currentContainer.elementSchema, size))
    }

    private fun push(encoder: JsonElementEncoder) {
        containerStack.addLast(currentContainer)
        currentContainer = encoder
    }

    override fun startItem() {}

    override fun writeArrayEnd() {}

    fun addItem(item: JsonElement) {
        determineCurrentContainer()
        currentContainer.addItem(item)
        endChildContainerIfNeeded()
    }

    fun determineCurrentContainer() {
        while (currentContainer.elementSchema.type == Schema.Type.RECORD) {
            push(JsonRecordEncoder(currentContainer.elementSchema))
        }
    }

    private fun endChildContainerIfNeeded() {
        if (currentContainer.isAllWritten && containerStack.isNotEmpty()) {
            endChildContainer()
        }
    }

    private fun endChildContainer() {
        val parentContainer = containerStack.removeLast()
        val childContainer = currentContainer
        currentContainer = parentContainer
        parentContainer.addItem(childContainer.element)
        endChildContainerIfNeeded()
    }

    override fun writeMapStart(size: Int) {
        determineCurrentContainer()
        push(JsonMapEncoder(currentContainer.elementSchema, size))
    }

    override fun writeMapEnd() {}

    override fun writeIndex(unionIndex: Int) {
        determineCurrentContainer()
        push(JsonUnionEncoder(currentContainer.elementSchema, unionIndex))
    }

    override fun flush() {
        if (containerStack.isNotEmpty()) throw SerializationException("The JSON model is currently incomplete. Flushing not possible.")
        val jsonElement = currentContainer.element
        json.encodeToStream(jsonElement, out.asOutputStream())
    }

    interface JsonElementEncoder {
        val element: JsonElement
        val elementSchema: Schema
        val isAllWritten: Boolean
        fun addItem(item: JsonElement)
    }

    class JsonRootEncoder(override val elementSchema: Schema) : JsonElementEncoder {
        override var element: JsonElement = JsonNull
        override var isAllWritten: Boolean = false

        override fun addItem(item: JsonElement) {
            this.element = item
            isAllWritten = true
        }
    }

    class JsonArrayEncoder(arraySchema: Schema, val size: Int) : JsonElementEncoder {
        val items = ArrayList<JsonElement>(size)
        override val element: JsonArray = JsonArray(items)
        override val elementSchema: Schema = arraySchema.elementType
        override val isAllWritten: Boolean
            get() = items.size == size

        override fun addItem(item: JsonElement) {
            items.add(item)
        }

    }

    class JsonRecordEncoder(val schema: Schema) : JsonElementEncoder {
        var index: Int = 0
        val items = LinkedHashMap<String, JsonElement>(schema.fields.size * 2)
        override val element: JsonElement = JsonObject(items)
        override val isAllWritten: Boolean
            get() = index == schema.fields.size
        override val elementSchema: Schema
            get() = schema.fields[index].schema()
        override fun addItem(item: JsonElement) {
            items[schema.fields[index++].name()] = item
        }
    }

    class JsonMapEncoder(val schema: Schema, val size: Int) : JsonElementEncoder {
        
        val items = LinkedHashMap<String, JsonElement>(size * 2)
        override val elementSchema: Schema 
            get() = if(currentFieldName == null) stringSchema else schema.valueType
        override val element: JsonElement = JsonObject(items)
        override val isAllWritten: Boolean
            get() = items.size == size
        var currentFieldName: String? = null

        override fun addItem(item: JsonElement) {
            val fieldName = currentFieldName
            if (fieldName == null && item is JsonPrimitive) {
                currentFieldName = item.content
            } else if (fieldName == null) {
                throw IllegalStateException("Field name has not been set.")
            } else {
                items[fieldName] = item
                currentFieldName = null
            }

        }
    }

    class JsonUnionEncoder(schema: Schema, indexOfActualSchema: Int) : JsonElementEncoder {
        override var isAllWritten: Boolean = false
        override var element: JsonElement = JsonNull
        override val elementSchema: Schema = schema.types[indexOfActualSchema]
        override fun addItem(item: JsonElement) {
            element = when (item) {
                is JsonNull -> item
                else -> JsonObject(mapOf(elementSchema.fullName to item))
            }
            isAllWritten = true
        }

    }

    private fun ByteArray.toJsonString(): String = this.toString(Charsets.ISO_8859_1)
}
