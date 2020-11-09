package com.sksamuel.avro4k.io

import kotlinx.serialization.SerializationException

interface AvroInputStream<T> : AutoCloseable {

   /**
    * Returns a [Sequence] for the values of T in the stream.
    * This function should not be invoked if using [next].
    */
   fun iterator(): Iterator<T> = iterator<T> {
      var next = next()
      while (next != null) {
         yield(next)
         next = next()
      }
   }

   /**
    * Returns the next value of T in the stream.
    * This function should not be invoked if using [iterator].
    */
   fun next(): T?

   fun nextOrThrow(): T = next() ?: throw SerializationException("No next entity found")
}