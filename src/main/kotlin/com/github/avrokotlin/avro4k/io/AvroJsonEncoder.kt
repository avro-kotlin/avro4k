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
        currentContainer = RootEncoder(schema)
        containerStack.clear()
    }

    override fun writeNull() = currentContainer.addItem(JsonNull)

    override fun writeBoolean(b: Boolean) = currentContainer.addItem(JsonPrimitive(b))

    override fun writeInt(n: Int) = currentContainer.addItem(JsonPrimitive(n))

    override fun writeLong(n: Long) = currentContainer.addItem(JsonPrimitive(n))

    override fun writeFloat(f: Float) = currentContainer.addItem(JsonPrimitive(f))

    override fun writeDouble(d: Double) = currentContainer.addItem(JsonPrimitive(d))

    override fun writeByte(value: Byte) = currentContainer.addItem(JsonPrimitive(value))

    override fun writeString(cs: CharSequence) {
        val str = cs.toString()
        val container = currentContainer
        if (container is MapEncoder && container.currentFieldName == null) {
            container.currentFieldName = str
        } else {
            currentContainer.addItem(JsonPrimitive(str))
        }
    }

    override fun writeBytes(bytes: Buffer) = writeBytes(bytes.readByteArray())


    override fun writeBytes(bytes: ByteArray) = currentContainer.addItem(JsonPrimitive(bytes.toHexString()))

    override fun writeFixed(bytes: Buffer) = writeBytes(bytes)

    override fun writeFixed(bytes: ByteArray) = writeBytes(bytes)

    override fun writeEnum(e: Int) = writeString(currentContainer.elementSchema.enumSymbols[e])

    override fun writeArrayStart(size: Int) {
        val arrayEncoder = ArrayEncoder(currentContainer.elementSchema, size)
        containerStack.addLast(currentContainer)
        currentContainer = arrayEncoder
    }

    override fun startItem() {}

    override fun writeArrayEnd() = endChildContainer()

    fun writeObjectStart(firstItem: JsonElement) {
        val objectEncoder = ObjectEncoder(currentContainer.elementSchema, this)
        containerStack.addLast(objectEncoder)
        currentContainer = objectEncoder
        objectEncoder.addItem(firstItem)
    }

    private fun endChildContainer() {
        val parentContainer = containerStack.removeLast()
        val childContainer = currentContainer
        currentContainer = parentContainer
        parentContainer.addItem(childContainer.element)
    }

    override fun writeMapStart(size: Int) {
        val mapEncoder = MapEncoder(currentContainer.elementSchema, size)
        containerStack.add(currentContainer)
        currentContainer = mapEncoder
    }

    override fun writeMapEnd() = endChildContainer()

    override fun writeIndex(unionIndex: Int) {
        val unionEncoder = UnionEncoder(currentContainer.elementSchema, unionIndex) {
            endChildContainer()
        }
        containerStack.add(currentContainer)
        currentContainer = unionEncoder
    }

    override fun flush() {
        if (containerStack.isNotEmpty()) throw SerializationException("The JSON model is currently incomplete. Flushing not possible.")
        val jsonElement = currentContainer.element
        json.encodeToStream(jsonElement, out.asOutputStream())
    }

    interface JsonElementEncoder {
        val element: JsonElement
        val elementSchema: Schema
        fun addItem(item: JsonElement)
    }

    class RootEncoder(override val elementSchema: Schema) : JsonElementEncoder {
        override var element: JsonElement = JsonNull
        override fun addItem(item: JsonElement) {
            this.element = item
        }
    }

    class ArrayEncoder(arraySchema: Schema, size: Int) : JsonElementEncoder {
        val items = ArrayList<JsonElement>(size)
        override val element: JsonArray = JsonArray(items)
        override val elementSchema: Schema = arraySchema.elementType
        override fun addItem(item: JsonElement) {
            items.add(item)
        }

    }

    class ObjectEncoder(val schema: Schema, val avroJsonEncoder: AvroJsonEncoder) : JsonElementEncoder {
        var index: Int = 0
        val items = HashMap<String, JsonElement>(schema.fields.size * 2)
        override val element: JsonElement = JsonObject(items)
        override val elementSchema: Schema = schema.fields[index].schema()
        override fun addItem(item: JsonElement) {
            if (elementSchema.type == Schema.Type.RECORD && item !is JsonObject) {
                avroJsonEncoder.writeObjectStart(item)
            } else {
                items[schema.fields[index++].name()] = item
                if (index == schema.fields.size) {
                    avroJsonEncoder.endChildContainer()
                }
            }
        }
    }

    class MapEncoder(schema: Schema, size: Int) : JsonElementEncoder {
        val items = HashMap<String, JsonElement>(size * 2)
        override val elementSchema: Schema = schema.elementType
        override val element: JsonElement = JsonObject(items)
        var currentFieldName: String? = null

        override fun addItem(item: JsonElement) {
            val fieldName = currentFieldName ?: throw IllegalStateException("Field name has not been set.")
            items[fieldName] = item
            currentFieldName = null
        }
    }

    class UnionEncoder(schema: Schema, indexOfActualSchema: Int, val doAfterAddItem: () -> Unit) : JsonElementEncoder {
        override var element: JsonElement = JsonNull
        override val elementSchema: Schema = schema.types[indexOfActualSchema]
        override fun addItem(item: JsonElement) {
            element = when (item) {
                is JsonNull -> item
                else -> JsonObject(mapOf(elementSchema.name to item))
            }
            doAfterAddItem.invoke()
        }

    }
}
private val HEX_CHARS = "0123456789abcdef".toCharArray()

fun ByteArray.toHexString() : String{
    val result = StringBuffer("\\u")

    forEach {
        val octet = it.toInt()
        val firstIndex = (octet and 0xF0).ushr(4)
        val secondIndex = octet and 0x0F
        result.append(HEX_CHARS[firstIndex])
        result.append(HEX_CHARS[secondIndex])
    }

    return result.toString()
}