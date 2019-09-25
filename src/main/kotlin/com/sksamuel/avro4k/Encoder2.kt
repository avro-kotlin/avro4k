package com.sksamuel.avro4k

import kotlinx.serialization.SerialDescriptor
import org.apache.avro.Schema

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

//
//val UUIDEncoder: Encoder2<UUID> = StringEncoder.comap { it.toString() }
//val LocalTimeEncoder: Encoder2<LocalTime> = IntEncoder.comap { it.toSecondOfDay() * 1000 + it.nano / 1000 }
//val LocalDateEncoder: Encoder2<LocalDate> = IntEncoder.comap { it.toEpochDay().toInt() }
//val InstantEncoder: Encoder2<Instant> = LongEncoder.comap(Instant::toEpochMilli)
//val LocalDateTimeEncoder: Encoder2<LocalDateTime> = InstantEncoder.comap { it.toInstant(ZoneOffset.UTC) }
//val TimestampEncoder: Encoder2<Timestamp> = InstantEncoder.comap(Timestamp::toInstant)
//val DateEncoder: Encoder2<java.sql.Date> = LocalDateEncoder.comap(java.sql.Date::toLocalDate)