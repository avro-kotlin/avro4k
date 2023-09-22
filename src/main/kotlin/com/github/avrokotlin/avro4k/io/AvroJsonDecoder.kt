package com.github.avrokotlin.avro4k.io

import kotlinx.io.Source
import kotlinx.io.asInputStream
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.*
import org.apache.avro.Schema

@OptIn(ExperimentalSerializationApi::class)
class AvroJsonDecoder(val source: Source) : AvroDecoder() {
    private var decoderStack = ArrayDeque<JsonElementDecoder>()
    private lateinit var currentDecoder: JsonElementDecoder
    private lateinit var writeSchema : Schema

    override fun configure(writeSchema: Schema) {
        this.writeSchema = writeSchema
        currentDecoder = initDecoderFromSource()
        decoderStack.clear()
    }

    override fun readNull() {
        //Do nothing
    }

    override fun readBoolean(): Boolean = decodePrimitive().boolean
    override fun readInt(): Int = decodePrimitive().int
    override fun readLong(): Long = decodePrimitive().long
    override fun readFloat(): Float = decodePrimitive().float
    override fun readDouble(): Double = decodePrimitive().double
    override fun readString(): String = decodePrimitive().content
    override fun readFixedString(length: Long): String = readString()
    override fun skipString() {
        readString()
    }

    @OptIn(ExperimentalStdlibApi::class)
    override fun readBytes(): ByteArray = decodePrimitive().content.hexStringToByteArray()

    override fun skipBytes() {
        decodePrimitive()
    }

    private fun decodeNextElement(): JsonElement {
        if (currentDecoder.isAllRead) {
            currentDecoder = decoderStack.removeLastOrNull() ?: initDecoderFromSource()
        }
        val nextElement = currentDecoder.nextElement()
        if (nextElement is JsonObject && currentDecoder.elementSchema.type == Schema.Type.RECORD) {
            push(RecordDecoder(nextElement, currentDecoder.elementSchema))
            return decodeNextElement()
        }
        return nextElement
    }

    private fun push(decoder: JsonElementDecoder) {
        if (decoder.isAllRead) return
        decoderStack.addLast(currentDecoder)
        currentDecoder = decoder
    }

    private fun decodePrimitive() = decodeNextElement().jsonPrimitive
    private fun initDecoderFromSource(): JsonElementDecoder {
        return RootDecoder(json.decodeFromStream<JsonElement>(source.asInputStream()), writeSchema)
    }

    override fun readFixed(length: Int): ByteArray = readBytes()

    override fun skipFixed(length: Int) {
        readFixed(length)
    }

    override fun readEnum(): Int {
        val symbol = readString()
        val schema = currentDecoder.elementSchema
        return schema.enumSymbols.indexOf(symbol)
    }

    override fun readArrayStart(): Long {
        val element = decodeNextElement()
        val jsonArray = element as? JsonArray
            ?: throw SerializationException("Cannot read array start because $element is no json array.")
        if (jsonArray.isEmpty()) return 0
        push(ListDecoder(jsonArray, currentDecoder.elementSchema))
        return jsonArray.size.toLong()
    }

    override fun arrayNext(): Long = 0L

    override fun skipArray(): Long {
        val element = decodeNextElement()
        if (element !is JsonArray) throw SerializationException("Cannot read array start because $element is no json array.")
        return 0L
    }

    override fun readMapStart(): Long {
        val element = decodeNextElement()
        val jsonObject = element as? JsonObject
            ?: throw SerializationException("Cannot read map start because $element is no json object.")
        if (jsonObject.isEmpty()) return 0
        push(MapDecoder(jsonObject, currentDecoder.elementSchema))
        return jsonObject.size.toLong()
    }

    override fun mapNext(): Long = 0

    override fun skipMap(): Long {
        val element = decodeNextElement()
        val jsonObject = element as? JsonObject
            ?: throw SerializationException("Cannot read map start because $element is no json object.")
        return 0L
    }

    override fun readIndex(): Int {
        val element = decodeNextElement()
        val unionDecoder = UnionDecoder(element, currentDecoder.elementSchema)
        push(unionDecoder)
        return unionDecoder.unionIndex
    }

    interface JsonElementDecoder {
        val elementSchema: Schema
        val isAllRead: Boolean
        fun nextElement(): JsonElement
    }

    class RootDecoder(val jsonElement: JsonElement, override val elementSchema: Schema) : JsonElementDecoder {
        override var isAllRead: Boolean = false
        override fun nextElement() = jsonElement.also { isAllRead = true }

    }

    class RecordDecoder(val jsonObject: JsonObject, val schema: Schema) : JsonElementDecoder {
        var index: Int = -1
        val currentField
            get() = schema.fields[index]
        override val elementSchema
            get() = currentField.schema()
        override val isAllRead: Boolean
            get() = index + 1 == schema.fields.size

        override fun nextElement(): JsonElement {
            index++
            return jsonObject[currentField.name()]
                ?: throw SerializationException("Could not decode field " + currentField.name() + " because it does not exists in $jsonObject.")
        }
    }

    class ListDecoder(val jsonArray: JsonArray, schema: Schema) : JsonElementDecoder {
        override val elementSchema: Schema = schema.elementType
        override val isAllRead: Boolean
            get() = index == jsonArray.size
        private var index: Int = 0
        override fun nextElement(): JsonElement {
            return jsonArray[index++]
        }
    }

    class MapDecoder(jsonObject: JsonObject, schema: Schema) : JsonElementDecoder {
        override val elementSchema: Schema = schema.elementType
        private var index: Int = 0
        private var keyNotDecoded = true
        private val mapEntries = jsonObject.entries.toList()
        override val isAllRead: Boolean
            get() = index == mapEntries.size

        override fun nextElement(): JsonElement {
            val entry = mapEntries[index]
            return if (keyNotDecoded) {
                keyNotDecoded = false
                JsonPrimitive(entry.key)
            } else {
                index++
                entry.value
            }
        }

    }

    class UnionDecoder(val jsonElement: JsonElement, val unionSchema: Schema) : JsonElementDecoder {
        private val indexAndSchema: IndexedValue<Schema> by lazy {
            when (jsonElement) {
                is JsonNull -> {
                    isAllRead = true
                    unionSchema.types.withIndex().first { it.value.type == Schema.Type.NULL }
                }

                is JsonObject -> {
                    if (jsonElement.size != 1) {
                        throw SerializationException("Cannot decode a JsonObject with size ${jsonElement.size} as a Union. A Union object must have only one property.")
                    }
                    val name = jsonElement.keys.first()
                    unionSchema.types.withIndex().first { it.value.name == name }
                }

                else -> throw SerializationException("Cannot decode $jsonElement as a Union. It needs to either be 'null' or a json object.")
            }
        }
        val unionIndex: Int = indexAndSchema.index
        override val elementSchema: Schema = indexAndSchema.value
        override var isAllRead: Boolean = false
        override fun nextElement(): JsonElement {
            return (jsonElement as JsonObject).values.first().also { isAllRead = true }
        }

    }
}

private val HEX_CHARS = "0123456789abcdef"

fun String.hexStringToByteArray() : ByteArray {
    val hexStr = substring(2) //remove \u in front of hex string
    val length = hexStr.length
    val result = ByteArray(length / 2)

    for (i in 0 until length step 2) {
        val firstIndex = HEX_CHARS.indexOf(hexStr[i]);
        val secondIndex = HEX_CHARS.indexOf(hexStr[i + 1]);

        val octet = firstIndex.shl(4).or(secondIndex)
        result.set(i.shr(1), octet.toByte())
    }

    return result
}