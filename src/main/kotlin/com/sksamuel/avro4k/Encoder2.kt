package com.sksamuel.avro4k

import kotlinx.serialization.CompositeEncoder
import kotlinx.serialization.ElementValueEncoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.StructureKind
import kotlinx.serialization.modules.EmptyModule
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.*

class AvroEncoder(
    private val schema: Schema,
    override val context: SerialModule = EmptyModule
) : ElementValueEncoder() {

  object StringEncoder

  private val values = ArrayList<Any>()
  private var index = 0
  private var encoder: CompositeEncoder? = null

  override fun encodeElement(desc: SerialDescriptor, index: Int): Boolean {
    this.index = index
    // we may have delegated in the previous element
    println("encodeElement ${desc.name} $index")
    if (desc.kind is StructureKind.CLASS) {
      //key = desc.getElementName(index)
    }
    return true
  }

  override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
    println("encodeSerializableValue $serializer $value")
    super.encodeSerializableValue(serializer, value)
  }

  override fun beginCollection(desc: SerialDescriptor,
                               collectionSize: Int,
                               vararg typeParams: KSerializer<*>): CompositeEncoder {
    println("beginCollection ${desc.name} $collectionSize $typeParams")
    return super.beginCollection(desc, collectionSize, *typeParams)
  }

  override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeEncoder {
    println("beginStructure ${desc.name}")
    val subschema = schema.fields[index].schema()
    return when (desc.kind as StructureKind) {
      is StructureKind.LIST -> ListEncoder(subschema).apply { encoder = this }
      else -> super.beginStructure(desc, *typeParams)
    }
  }

  override fun endStructure(desc: SerialDescriptor) {
    println("endStructure ${desc.name} encoder=$encoder")
    when (desc.kind) {
      is StructureKind.LIST -> values.add((encoder as ListEncoder).result())
      else -> super.endStructure(desc)
    }
  }

  override fun encodeValue(value: Any) {
    println("encodeValue $value")
  }

  override fun encodeString(value: String) {
    val encoded = com.sksamuel.avro4k.StringEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeLong(value: Long) {
    val encoded = LongEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeDouble(value: Double) {
    val encoded = DoubleEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeBoolean(value: Boolean) {
    val encoded = BooleanEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeShort(value: Short) {
    val encoded = ShortEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeByte(value: Byte) {
    val encoded = ByteEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeFloat(value: Float) {
    val encoded = FloatEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeInt(value: Int) {
    val encoded = IntEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  fun record(): Record {
    return ImmutableRecord(schema, values)
  }
}

class ListEncoder(private val schema: Schema) : ElementValueEncoder() {

  private val values = mutableListOf<Any>()

  fun result(): GenericData.Array<Any> {
    println("Building list from $values")
    return GenericData.Array<Any>(schema, values.toList())
  }

  override fun endStructure(desc: SerialDescriptor) {
    println("endStructure ${desc.name} encoder=$this")
  }

  override fun encodeString(value: String) {
    val encoded = StringEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeLong(value: Long) {
    val encoded = LongEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeDouble(value: Double) {
    val encoded = DoubleEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeBoolean(value: Boolean) {
    val encoded = BooleanEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeShort(value: Short) {
    val encoded = ShortEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeByte(value: Byte) {
    val encoded = ByteEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeFloat(value: Float) {
    val encoded = FloatEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }

  override fun encodeInt(value: Int) {
    val encoded = IntEncoder.encode(value, schema, DefaultNamingStrategy)
    values.add(encoded)
  }
}

interface Encoder2<T> {

  /**
   * @param namingStrategy the [[NamingStrategy]] is used when encoding container types
   *                    with nested fields. Fields may have a different name in the
   *                    outgoing message compared to the class field names, and the
   *                    fieldMapper is used to map between them.
   */
  fun encode(value: T, schema: Schema, namingStrategy: NamingStrategy): Any

  fun <S> comap(f: (S) -> T): Encoder2<S> = object : Encoder2<S> {
    override fun encode(value: S, schema: Schema, namingStrategy: NamingStrategy): Any =
        this@Encoder2.encode(f(value), schema, namingStrategy)
  }
}

inline fun <reified T> encoderFor(): Encoder2<T> = when (T::class) {
  String::class -> StringEncoder as Encoder2<T>
  Long::class -> LongEncoder as Encoder2<T>
  Int::class -> IntEncoder as Encoder2<T>
  Double::class -> DoubleEncoder as Encoder2<T>
  Float::class -> FloatEncoder as Encoder2<T>
  Boolean::class -> BooleanEncoder as Encoder2<T>
  LocalDate::class -> LocalDateEncoder as Encoder2<T>
  Instant::class -> InstantEncoder as Encoder2<T>
  LocalTime::class -> LocalTimeEncoder as Encoder2<T>
  Timestamp::class -> TimestampEncoder as Encoder2<T>
  Date::class -> DateEncoder as Encoder2<T>
  UUID::class -> UUIDEncoder as Encoder2<T>
  else -> throw IllegalArgumentException("No encoder available for type ${T::class}")
}

class ClassEncoder<T>(private val descriptor: SerialDescriptor) : Encoder2<T> {

  override fun encode(value: T, schema: Schema, namingStrategy: NamingStrategy): Any {
    TODO()
  }

//  /**
//   * Takes the encoded values from the fields of a type T and builds
//   * an [[ImmutableRecord]] from them, using the given schema.
//   *
//   * The schema for a record must be of Type Schema.Type.RECORD
//   */
//  fun buildRecord(schema: Schema, values: List<Any>): Record {
//    require(schema.type == Schema.Type.RECORD) { "Trying to encode a field from schema $schema which is not a RECORD" }
//    return ImmutableRecord(schema, values)
//  }
}

object StringEncoder : Encoder2<String> {
  override fun encode(value: String, schema: Schema, namingStrategy: NamingStrategy): Any {
    return when (schema.type) {
      Schema.Type.FIXED -> {
        if (value.toByteArray().size > schema.fixedSize)
          throw RuntimeException("Cannot write string with ${value.toByteArray().size} bytes to fixed type of size ${schema.fixedSize}")
        GenericData.get().createFixed(
            null,
            ByteBuffer.allocate(schema.fixedSize).put(value.toByteArray()).array(),
            schema
        )
      }
      Schema.Type.BYTES -> ByteBuffer.wrap(value.toByteArray())
      else -> Utf8(value)
    }
  }
}

object BooleanEncoder : Encoder2<Boolean> {
  override fun encode(value: Boolean, schema: Schema, namingStrategy: NamingStrategy): Boolean =
      java.lang.Boolean.valueOf(value)
}

object IntEncoder : Encoder2<Int> {
  override fun encode(value: Int, schema: Schema, namingStrategy: NamingStrategy): Int = Integer.valueOf(value)
}

object LongEncoder : Encoder2<Long> {
  override fun encode(value: Long, schema: Schema, namingStrategy: NamingStrategy): Long =
      java.lang.Long.valueOf(value)
}

object FloatEncoder : Encoder2<Float> {
  override fun encode(value: Float, schema: Schema, namingStrategy: NamingStrategy): Float =
      java.lang.Float.valueOf(value)
}

object DoubleEncoder : Encoder2<Double> {
  override fun encode(value: Double, schema: Schema, namingStrategy: NamingStrategy): Double =
      java.lang.Double.valueOf(value)
}

object ShortEncoder : Encoder2<Short> {
  override fun encode(value: Short, schema: Schema, namingStrategy: NamingStrategy): Short =
      java.lang.Short.valueOf(value)
}

object ByteEncoder : Encoder2<Byte> {
  override fun encode(value: Byte, schema: Schema, namingStrategy: NamingStrategy): Byte =
      java.lang.Byte.valueOf(value)
}

val UUIDEncoder: Encoder2<UUID> = StringEncoder.comap { it.toString() }
val LocalTimeEncoder: Encoder2<LocalTime> = IntEncoder.comap { it.toSecondOfDay() * 1000 + it.nano / 1000 }
val LocalDateEncoder: Encoder2<LocalDate> = IntEncoder.comap { it.toEpochDay().toInt() }
val InstantEncoder: Encoder2<Instant> = LongEncoder.comap(Instant::toEpochMilli)
val LocalDateTimeEncoder: Encoder2<LocalDateTime> = InstantEncoder.comap { it.toInstant(ZoneOffset.UTC) }
val TimestampEncoder: Encoder2<Timestamp> = InstantEncoder.comap(Timestamp::toInstant)
val DateEncoder: Encoder2<java.sql.Date> = LocalDateEncoder.comap(java.sql.Date::toLocalDate)